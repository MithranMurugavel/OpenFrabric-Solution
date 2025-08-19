from flask import Flask, request, jsonify
from flask_cors import CORS # To handle Cross-Origin Resource Sharing
from reconciliation import process_settlement_report, get_dashboard_summary, get_all_transactions, get_transaction_details
from database import create_tables, seed_initial_transactions
import os

app = Flask(__name__)
CORS(app) # Enable CORS for all routes

# Initialize database on startup
@app.before_request
def initialize_database():
    create_tables()
    seed_initial_transactions()

@app.route('/reconcile', methods=['POST'])
def reconcile_settlements():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    if file:
        # Save the uploaded file temporarily
        file_path = os.path.join('data', file.filename)
        if not os.path.exists('data'):
            os.makedirs('data')
        file.save(file_path)
        
        processed_count = process_settlement_report(file_path)
        os.remove(file_path) # Clean up the temporary file
        return jsonify({"message": f"Successfully processed {processed_count} settlements."}), 200
    return jsonify({"error": "Something went wrong"}), 500

@app.route('/transactions', methods=['GET'])
def list_transactions():
    transactions = get_all_transactions()
    return jsonify(transactions), 200

@app.route('/transactions/<string:transaction_id>', methods=['GET'])
def get_single_transaction(transaction_id):
    transaction = get_transaction_details(transaction_id)
    if transaction:
        return jsonify(transaction), 200
    return jsonify({"error": "Transaction not found"}), 404

@app.route('/dashboard/summary', methods=['GET'])
def get_dashboard_summary_data():
    summary = get_dashboard_summary()
    return jsonify(summary), 200

if __name__ == '__main__':
    app.run(debug=True, port=5000) # Run in debug mode for development
