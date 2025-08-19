import sqlite3
from datetime import datetime

DATABASE_NAME = 'reconciliation.db'

def get_db_connection():
    conn = sqlite3.connect(DATABASE_NAME)
    conn.row_factory = sqlite3.Row # This allows accessing columns by name
    return conn

def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCHAR(50) PRIMARY KEY,
            lifecycle_id VARCHAR(50),
            account_id VARCHAR(50) NOT NULL,
            merchant_name VARCHAR(100) NOT NULL,
            transaction_date DATE NOT NULL,
            transaction_amount DECIMAL(10, 2) NOT NULL,
            currency VARCHAR(3) NOT NULL,
            status VARCHAR(20) NOT NULL,
            settlement_status VARCHAR(20) DEFAULT 'PENDING',
            total_settled_amount DECIMAL(10, 2) DEFAULT 0.00,
            last_settlement_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS settlement_history (
            settlement_id VARCHAR(50) PRIMARY KEY,
            transaction_id VARCHAR(50),
            lifecycle_id VARCHAR(50),
            settlement_date DATE NOT NULL,
            settlement_amount DECIMAL(10, 2) NOT NULL,
            settlement_type VARCHAR(10) NOT NULL, -- 'DEBIT' or 'CREDIT'
            currency VARCHAR(3) NOT NULL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
        );
    ''')
    conn.commit()
    conn.close()

def seed_initial_transactions():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Check if transactions already exist to prevent duplicate inserts
    cursor.execute("SELECT COUNT(*) FROM transactions")
    if cursor.fetchone()[0] == 0:
        transactions_data = [
            ('TXN001', 'LC001', 'ACC123', 'Amazon.com', '2025-08-14', 125.99, 'USD', 'COMPLETED', 'PENDING', 0.00, None),
            ('TXN002', 'LC002', 'ACC124', 'Starbucks', '2025-08-14', 4.75, 'USD', 'COMPLETED', 'PENDING', 0.00, None),
            ('TXN003', 'LC003', 'ACC125', 'Shell Gas', '2025-08-14', 45.30, 'USD', 'COMPLETED', 'PENDING', 0.00, None),
            ('TXN004', 'LC004', 'ACC126', 'Best Buy', '2025-08-14', 899.99, 'USD', 'COMPLETED', 'PENDING', 0.00, None),
            ('TXN005', 'LC005', 'ACC127', 'Home Depot', '2025-08-14', 234.50, 'USD', 'COMPLETED', 'PENDING', 0.00, None),
            ('TXN006', 'LC006', 'ACC128', 'Target', '2025-08-14', 87.25, 'USD', 'COMPLETED', 'PENDING', 0.00, None),
            ('TXN007', 'LC007', 'ACC129', 'Walmart', '2025-08-14', 156.78, 'USD', 'COMPLETED', 'PENDING', 0.00, None),
            ('TXN008', 'LC008', 'ACC130', 'Apple', '2025-08-05', 1299.99, 'USD', 'COMPLETED', 'PENDING', 0.00, None),
            ('TXN009', 'LC009', 'ACC131', 'Nike', '2025-08-14', 189.95, 'USD', 'COMPLETED', 'PENDING', 0.00, None),
            ('TXN010', 'LC010', 'ACC132', 'Netflix', '2025-08-07', 15.99, 'USD', 'COMPLETED', 'PENDING', 0.00, None),
            ('TXN013', 'LC011', 'ACC135', 'Corner Deli', '2025-08-14', 32.45, 'USD', 'COMPLETED', 'PENDING', 0.00, None),
            ('TXN014', 'LC012', 'ACC136', 'Shell Gas', '2025-08-14', 28.90, 'USD', 'COMPLETED', 'PENDING', 0.00, None),
            ('TXN015', 'LC013', 'ACC137', 'Amazon.com', '2025-08-14', 199.99, 'USD', 'FAILED', 'NOT_APPLICABLE', 0.00, None),
            ('TXN016', 'LC014', 'ACC138', 'Uber Taxi', '2025-08-14', 75.25, 'USD', 'DECLINED', 'NOT_APPLICABLE', 0.00, None)
        ]
        cursor.executemany('''
            INSERT INTO transactions (transaction_id, lifecycle_id, account_id, merchant_name, transaction_date,
                                      transaction_amount, currency, status, settlement_status, total_settled_amount,
                                      last_settlement_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', transactions_data)
        conn.commit()
        print("Initial transactions seeded.")
    else:
        print("Transactions already exist in the database. Skipping seeding.")
    conn.close()

if __name__ == '__main__':
    create_tables()
    seed_initial_transactions()
