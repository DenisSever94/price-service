package price.service.service;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import price.service.model.AveragePrice;
import price.service.model.PriceUpdate;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class DatabaseService {
	
	private static final Logger logger = LoggerFactory.getLogger(DatabaseService.class);
	
	private final DataSource dataSource;
	private final ReadWriteLock lock = new ReentrantReadWriteLock();
	
	public DatabaseService() {
		this.dataSource = createDataSource();
		initializeDatabase();
	}
	
	private static final String PROPERTIES_FILE = "database.properties";
	
	public DataSource createDataSource() {
		Properties properties = new Properties();
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(properties.getProperty("db.url"));
		config.setUsername(properties.getProperty("db.username"));
		config.setPassword(properties.getProperty("db.password"));
		config.setMaximumPoolSize(Integer.parseInt(properties.getProperty("db.pool.maxSize")));
		config.setMinimumIdle(Integer.parseInt(properties.getProperty("db.pool.minIdle")));
		config.setConnectionTimeout(Long.parseLong(properties.getProperty("db.pool.connectionTimeout")));
		config.setIdleTimeout(Long.parseLong(properties.getProperty("db.pool.idleTimeout")));
		config.setMaxLifetime(Long.parseLong(properties.getProperty("db.pool.maxLifetime")));
		
		return new HikariDataSource(config);
	}
	
	private void initializeDatabase() {
		try(Connection conn = dataSource.getConnection()) {
			
			String createPricesTable = """
					    CREATE TABLE IF NOT EXISTS product_prices (
					        id SERIAL PRIMARY KEY,
					        product_id BIGINT NOT NULL,
					        manufacturer_name VARCHAR(255) NOT NULL,
					        price DECIMAL(10,2) NOT NULL,
					        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
					        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
					        UNIQUE(product_id, manufacturer_name)
					    )
					""";
			
			String createAveragePricesTable = """
					    CREATE TABLE IF NOT EXISTS average_prices (
					        product_id BIGINT PRIMARY KEY,
					        average_price DECIMAL(10,2) NOT NULL,
					        offer_count INTEGER NOT NULL,
					        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
					    )
					""";
			
			String createIndexes = """
					    CREATE INDEX IF NOT EXISTS idx_product_prices_product_id ON product_prices(product_id);
					    CREATE INDEX IF NOT EXISTS idx_product_prices_manufacturer ON product_prices(manufacturer_name);
					""";
			
			try(Statement stmt = conn.createStatement()) {
				stmt.execute(createPricesTable);
				stmt.execute(createAveragePricesTable);
				stmt.execute(createIndexes);
				logger.info("Database initialized successfully");
			}
		} catch(SQLException e) {
			logger.error("Failed to initialize database", e);
			throw new RuntimeException("Database initialization failed", e);
		}
	}
	
	public void updatePrice(PriceUpdate priceUpdate) {
		lock.writeLock().lock();
		try(Connection conn = dataSource.getConnection()) {
			conn.setAutoCommit(false);
			
			try {
				String upsertSql = """
						    INSERT INTO product_prices (product_id, manufacturer_name, price, updated_at)
						    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
						    ON CONFLICT (product_id, manufacturer_name)
						    DO UPDATE SET price = EXCLUDED.price, updated_at = CURRENT_TIMESTAMP
						""";
				
				try(PreparedStatement stmt = conn.prepareStatement(upsertSql)) {
					stmt.setLong(1, priceUpdate.getProductId());
					stmt.setString(2, priceUpdate.getManufacturerName());
					stmt.setBigDecimal(3, priceUpdate.getPrice());
					stmt.executeUpdate();
				}
				
				recalculateAveragePrice(conn, priceUpdate.getProductId());
				
				conn.commit();
				logger.info("Price updated successfully for product {} from manufacturer {}",
						priceUpdate.getProductId(), priceUpdate.getManufacturerName());
				
			} catch(SQLException e) {
				conn.rollback();
				logger.error("Failed to update price", e);
				throw e;
			}
		} catch(SQLException e) {
			logger.error("Database error during price update", e);
			throw new RuntimeException("Failed to update price", e);
		} finally {
			lock.writeLock().unlock();
		}
	}
	
	private void recalculateAveragePrice(Connection conn, Long productId) throws SQLException {
		String avgSql = """
				    SELECT AVG(price) as avg_price, COUNT(*) as offer_count
				    FROM product_prices
				    WHERE product_id = ?
				""";
		
		try(PreparedStatement stmt = conn.prepareStatement(avgSql)) {
			stmt.setLong(1, productId);
			ResultSet rs = stmt.executeQuery();
			
			if(rs.next()) {
				Double avgPrice = rs.getDouble("avg_price");
				Integer offerCount = rs.getInt("offer_count");
				
				String upsertAvgSql = """
						    INSERT INTO average_prices (product_id, average_price, offer_count, updated_at)
						    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
						    ON CONFLICT (product_id)
						    DO UPDATE SET
						        average_price = EXCLUDED.average_price,
						        offer_count = EXCLUDED.offer_count,
						        updated_at = CURRENT_TIMESTAMP
						""";
				
				try(PreparedStatement avgStmt = conn.prepareStatement(upsertAvgSql)) {
					avgStmt.setLong(1, productId);
					avgStmt.setDouble(2, avgPrice);
					avgStmt.setInt(3, offerCount);
					avgStmt.executeUpdate();
				}
			}
		}
	}
	
	/**
	 * Получает среднюю цену товара
	 */
	public AveragePrice getAveragePrice(Long productId) {
		lock.readLock().lock();
		try(Connection conn = dataSource.getConnection();
			PreparedStatement stmt = conn.prepareStatement(
					"SELECT product_id, average_price, offer_count FROM average_prices WHERE product_id = ?")) {
			
			stmt.setLong(1, productId);
			ResultSet rs = stmt.executeQuery();
			
			if(rs.next()) {
				return new AveragePrice(
						rs.getLong("product_id"),
						rs.getBigDecimal("average_price"),
						rs.getInt("offer_count")
				);
			}
			return null;
		} catch(SQLException e) {
			logger.error("Failed to get average price for product {}", productId, e);
			throw new RuntimeException("Failed to get average price", e);
		} finally {
			lock.readLock().unlock();
		}
	}
	
	/**
	 * Получает все средние цены
	 */
	public List<AveragePrice> getAllAveragePrices() {
		lock.readLock().lock();
		try(Connection conn = dataSource.getConnection();
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(
					"SELECT product_id, average_price, offer_count FROM average_prices ORDER BY product_id")) {
			
			List<AveragePrice> prices = new ArrayList<>();
			while(rs.next()) {
				prices.add(new AveragePrice(
						rs.getLong("product_id"),
						rs.getBigDecimal("average_price"),
						rs.getInt("offer_count")
				));
			}
			return prices;
		} catch(SQLException e) {
			logger.error("Failed to get all average prices", e);
			throw new RuntimeException("Failed to get average prices", e);
		} finally {
			lock.readLock().unlock();
		}
	}
	
	/**
	 * Закрывает соединения с базой данных
	 */
	public void close() {
		if(dataSource instanceof HikariDataSource) {
			((HikariDataSource) dataSource).close();
		}
	}
} 