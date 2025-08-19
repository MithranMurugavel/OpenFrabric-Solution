import csv
from datetime import datetime, timedelta
from database import get_db_connection

def process_settlement_report(csv_file_path):
    conn = get_db_connection()
    cursor = conn.cursor()
    processed_settlements_count = 0

    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            settlement_id = row['settlement_id']
            lifecycle_id = row['lifecycle_id'] if row['lifecycle_id'] else None
            account_id = row['account_id']
            merchant_name = row['merchant_name']
            transaction_date_str = row['transaction_date']
            settlement_date_str = row['settlement_date']
            settlement_amount = float(row['settlement_amount'])
            settlement_type = row['settlement_type']
            currency = row['currency']

            # Find matching transaction
            transaction_id = None
            if lifecycle_id:
                cursor.execute("SELECT transaction_id FROM transactions WHERE lifecycle_id = ?", (lifecycle_id,))
                result = cursor.fetchone()
                if result:
                    transaction_id = result['transaction_id']
            
            if not transaction_id: # Fallback logic
                cursor.execute("""
                    SELECT transaction_id FROM transactions
                    WHERE account_id = ? AND merchant_name = ? AND transaction_date = ?
                """, (account_id, merchant_name, transaction_date_str))
                result = cursor.fetchone()
                if result:
                    transaction_id = result['transaction_id']

            if transaction_id:
                # Insert into settlement_history
                cursor.execute("""
                    INSERT INTO settlement_history (settlement_id, transaction_id, lifecycle_id, settlement_date,
                                                    settlement_amount, settlement_type, currency)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (settlement_id, transaction_id, lifecycle_id, settlement_date_str,
                      settlement_amount, settlement_type, currency))
                processed_settlements_count += 1
            else:
                print(f"Warning: No matching transaction found for settlement ID {settlement_id}. Skipping.")
    
    conn.commit()
    conn.close()
    print(f"Processed {processed_settlements_count} settlements from the report.")
    update_transaction_settlement_statuses()
    return processed_settlements_count

def update_transaction_settlement_statuses():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get all transactions that are not 'FAILED' or 'DECLINED'
    cursor.execute("SELECT transaction_id, transaction_amount, status FROM transactions WHERE status NOT IN ('FAILED', 'DECLINED')")
    transactions = cursor.fetchall()

    for txn in transactions:
        transaction_id = txn['transaction_id']
        transaction_amount = txn['transaction_amount']

        # Calculate total settled amount for the transaction
        cursor.execute("""
            SELECT SUM(CASE WHEN settlement_type = 'DEBIT' THEN settlement_amount ELSE -settlement_amount END) as net_settled_amount,
                   MAX(settlement_date) as latest_settlement_date
            FROM settlement_history
            WHERE transaction_id = ?
        """, (transaction_id,))
        settlement_summary = cursor.fetchone()

        net_settled_amount = settlement_summary['net_settled_amount'] if settlement_summary['net_settled_amount'] is not None else 0.00
        latest_settlement_date = settlement_summary['latest_settlement_date']

        new_settlement_status = 'PENDING'
        issue_flag = None

        if net_settled_amount == 0 and latest_settlement_date is None:
            new_settlement_status = 'PENDING'
            # Check for critical issue: No settlement after 7 days
            transaction_date_str = cursor.execute("SELECT transaction_date FROM transactions WHERE transaction_id = ?", (transaction_id,)).fetchone()['transaction_date']
            transaction_date = datetime.strptime(transaction_date_str, '%Y-%m-%d').date()
            if (datetime.now().date() - transaction_date).days > 7:
                issue_flag = 'CRITICAL: No settlement after 7 days'
        elif net_settled_amount < transaction_amount:
            new_settlement_status = 'PARTIAL'
            issue_flag = 'WARNING: Partial Settlement'
            if net_settled_amount <= 0 and transaction_amount > 0: # Consider if it's a full refund or more
                 new_settlement_status = 'REFUNDED' # If net settled is 0 or negative, and original was positive
        elif net_settled_amount == transaction_amount:
            new_settlement_status = 'FULLY_SETTLED'
        elif net_settled_amount > transaction_amount:
            new_settlement_status = 'OVER_SETTLED'
            issue_flag = 'CRITICAL: Over Settled'
            # Special case for REFUNDED: if net settlement is less than transaction amount due to credits
            # This is already covered by PARTIAL/REFUNDED logic above if net_settled_amount < transaction_amount
            # But if it's over-settled due to a credit that makes it less than original, it's REFUNDED
            if net_settled_amount < transaction_amount and any(s['settlement_type'] == 'CREDIT' for s in cursor.execute("SELECT settlement_type FROM settlement_history WHERE transaction_id = ?", (transaction_id,)).fetchall()):
                new_settlement_status = 'REFUNDED'
                issue_flag = None # No warning if it's a legitimate refund

        # Update transaction
        cursor.execute("""
            UPDATE transactions
            SET settlement_status = ?,
                total_settled_amount = ?,
                last_settlement_date = ?
            WHERE transaction_id = ?
        """, (new_settlement_status, net_settled_amount, latest_settlement_date, transaction_id))
        
        # Store issue flag (we'll add a column for this or handle it dynamically)
        # For now, we'll just print it or consider adding a new column to the transactions table
        if issue_flag:
            print(f"Transaction {transaction_id}: {issue_flag}")

    conn.commit()
    conn.close()
    print("Transaction settlement statuses updated.")

def get_transaction_issues(transaction_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT transaction_amount, transaction_date, total_settled_amount FROM transactions WHERE transaction_id = ?", (transaction_id,))
    txn = cursor.fetchone()
    
    if not txn:
        conn.close()
        return "Transaction not found."

    transaction_amount = txn['transaction_amount']
    total_settled_amount = txn['total_settled_amount']
    transaction_date_str = txn['transaction_date']
    transaction_date = datetime.strptime(transaction_date_str, '%Y-%m-%d').date()

    issues = []
    
    # Critical Issues
    if total_settled_amount > transaction_amount:
        issues.append("CRITICAL: Total settlement amount > transaction amount")
    
    # Check for no settlement after 7 days (only if total_settled_amount is 0)
    if total_settled_amount == 0:
        if (datetime.now().date() - transaction_date).days > 7:
            issues.append("CRITICAL: No settlement after 7 days from transaction date")

    # Warning Issues
    if total_settled_amount < transaction_amount and total_settled_amount > 0: # Exclude cases where it's 0 (pending) or negative (refunded)
        issues.append("WARNING: Total settlement amount < transaction amount")
        
    conn.close()
    return issues if issues else ["No issues detected."]

def get_dashboard_summary():
    conn = get_db_connection()
    cursor = conn.cursor()

    summary = {}

    cursor.execute("SELECT COUNT(*) FROM transactions")
    summary['total_transactions_processed'] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM settlement_history")
    summary['total_settlements_processed'] = cursor.fetchone()[0]

    cursor.execute("SELECT settlement_status, COUNT(*) FROM transactions GROUP BY settlement_status")
    summary['settlement_status_breakdown'] = {row['settlement_status']: row['COUNT(*)'] for row in cursor.fetchall()}

    # Calculate critical and warning issues dynamically
    critical_issues_count = 0
    warning_issues_count = 0
    
    cursor.execute("SELECT transaction_id, transaction_amount, total_settled_amount, transaction_date FROM transactions WHERE status NOT IN ('FAILED', 'DECLINED')")
    transactions_for_issues = cursor.fetchall()

    for txn in transactions_for_issues:
        txn_id = txn['transaction_id']
        txn_amount = txn['transaction_amount']
        settled_amount = txn['total_settled_amount']
        txn_date = datetime.strptime(txn['transaction_date'], '%Y-%m-%d').date()

        if settled_amount > txn_amount:
            critical_issues_count += 1
        
        if settled_amount == 0 and (datetime.now().date() - txn_date).days > 7:
            critical_issues_count += 1
            
        if settled_amount < txn_amount and settled_amount > 0:
            warning_issues_count += 1

    summary['num_critical_issues'] = critical_issues_count
    summary['num_warning_issues'] = warning_issues_count

    cursor.execute("SELECT SUM(transaction_amount - total_settled_amount) FROM transactions WHERE settlement_status IN ('PENDING', 'PARTIAL')")
    summary['total_outstanding_amount'] = cursor.fetchone()[0] or 0.00

    # Settlement performance metrics (simplified)
    cursor.execute("""
        SELECT AVG(JULIANDAY(settlement_date) - JULIANDAY(t.transaction_date))
        FROM settlement_history sh
        JOIN transactions t ON sh.transaction_id = t.transaction_id
        WHERE sh.settlement_type = 'DEBIT' AND t.settlement_status = 'FULLY_SETTLED'
    """)
    summary['avg_days_to_settle'] = round(cursor.fetchone()[0], 2) if cursor.fetchone()[0] else 0.00

    cursor.execute("SELECT CAST(COUNT(CASE WHEN settlement_status = 'FULLY_SETTLED' THEN 1 END) AS REAL) * 100 / COUNT(*) FROM transactions WHERE status = 'COMPLETED'")
    summary['settlement_rate'] = round(cursor.fetchone()[0], 2) if cursor.fetchone()[0] else 0.00

    conn.close()
    return summary

def get_all_transactions():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM transactions")
    transactions = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    # Add issue flags dynamically
    for txn in transactions:
        issues = get_transaction_issues(txn['transaction_id'])
        txn['issue_flag'] = ", ".join(issues) if issues else "No Issues"
        txn['issue_type'] = 'CRITICAL' if any('CRITICAL' in issue for issue in issues) else ('WARNING' if any('WARNING' in issue for issue in issues) else 'NONE')
    
    return transactions

def get_transaction_details(transaction_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM transactions WHERE transaction_id = ?", (transaction_id,))
    transaction = cursor.fetchone()
    
    if transaction:
        transaction = dict(transaction)
        cursor.execute("SELECT * FROM settlement_history WHERE transaction_id = ? ORDER BY settlement_date ASC", (transaction_id,))
        settlement_history = [dict(row) for row in cursor.fetchall()]
        
        transaction['settlement_history'] = settlement_history
        transaction['issue_details'] = get_transaction_issues(transaction_id)
        transaction['issue_type'] = 'CRITICAL' if any('CRITICAL' in issue for issue in transaction['issue_details']) else ('WARNING' if any('WARNING' in issue for issue in transaction['issue_details']) else 'NONE')
    
    conn.close()
    return transaction
