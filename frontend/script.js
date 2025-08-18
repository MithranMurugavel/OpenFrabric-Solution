const API_BASE_URL = 'http://127.0.0.1:5000'; // Flask default port

document.addEventListener('DOMContentLoaded', () => {
    loadDashboardSummary();
    loadTransactions();

    document.getElementById('uploadButton').addEventListener('click', uploadSettlementReport);
    document.getElementById('statusFilter').addEventListener('change', loadTransactions);

    const modal = document.getElementById('transactionDetailModal');
    const closeButton = document.querySelector('.close-button');

    closeButton.addEventListener('click', () => {
        modal.style.display = 'none';
    });

    window.addEventListener('click', (event) => {
        if (event.target == modal) {
            modal.style.display = 'none';
        }
    });
});

async function uploadSettlementReport() {
    const fileInput = document.getElementById('settlementFile');
    const uploadMessage = document.getElementById('uploadMessage');
    const file = fileInput.files[0];

    if (!file) {
        uploadMessage.textContent = 'Please select a CSV file to upload.';
        uploadMessage.style.color = 'red';
        return;
    }

    const formData = new FormData();
    formData.append('file', file);

    uploadMessage.textContent = 'Uploading and reconciling...';
    uploadMessage.style.color = 'blue';

    try {
        const response = await fetch(`${API_BASE_URL}/reconcile`, {
            method: 'POST',
            body: formData
        });

        const data = await response.json();

        if (response.ok) {
            uploadMessage.textContent = data.message;
            uploadMessage.style.color = 'green';
            loadDashboardSummary(); // Refresh dashboard after reconciliation
            loadTransactions(); // Refresh transaction list
        } else {
            uploadMessage.textContent = `Error: ${data.error}`;
            uploadMessage.style.color = 'red';
        }
    } catch (error) {
        console.error('Error uploading file:', error);
        uploadMessage.textContent = 'An error occurred during upload.';
        uploadMessage.style.color = 'red';
    }
}

async function loadDashboardSummary() {
    try {
        const response = await fetch(`${API_BASE_URL}/dashboard/summary`);
        const summary = await response.json();

        document.getElementById('totalTransactions').textContent = summary.total_transactions_processed;
        document.getElementById('totalSettlements').textContent = summary.total_settlements_processed;
        document.getElementById('criticalIssues').textContent = summary.num_critical_issues;
        document.getElementById('warningIssues').textContent = summary.num_warning_issues;
        document.getElementById('outstandingAmount').textContent = `$${summary.total_outstanding_amount.toFixed(2)}`;
        document.getElementById('avgDaysToSettle').textContent = summary.avg_days_to_settle;
        document.getElementById('settlementRate').textContent = summary.settlement_rate;

        const statusBreakdownList = document.getElementById('statusBreakdown');
        statusBreakdownList.innerHTML = '';
        for (const status in summary.settlement_status_breakdown) {
            const listItem = document.createElement('li');
            listItem.textContent = `${status}: ${summary.settlement_status_breakdown[status]}`;
            statusBreakdownList.appendChild(listItem);
        }

    } catch (error) {
        console.error('Error loading dashboard summary:', error);
    }
}

async function loadTransactions() {
    const statusFilter = document.getElementById('statusFilter').value;
    const transactionsTableBody = document.querySelector('#transactionsTable tbody');
    transactionsTableBody.innerHTML = ''; // Clear existing rows

    try {
        const response = await fetch(`${API_BASE_URL}/transactions`);
        let transactions = await response.json();

        if (statusFilter) {
            transactions = transactions.filter(txn => txn.settlement_status === statusFilter);
        }

        transactions.forEach(txn => {
            const row = transactionsTableBody.insertRow();
            row.insertCell().textContent = txn.transaction_id;
            row.insertCell().textContent = txn.transaction_date;
            row.insertCell().textContent = txn.merchant_name;
            row.insertCell().textContent = `$${txn.transaction_amount.toFixed(2)}`;
            
            const statusCell = row.insertCell();
            const statusBadge = document.createElement('span');
            statusBadge.classList.add('status-badge', `status-${txn.settlement_status.replace(/\s/g, '_')}`);
            statusBadge.textContent = txn.settlement_status;
            statusCell.appendChild(statusBadge);

            row.insertCell().textContent = `$${txn.total_settled_amount.toFixed(2)}`;
            
            const issueFlagCell = row.insertCell();
            const issueFlagSpan = document.createElement('span');
            issueFlagSpan.classList.add('issue-flag', txn.issue_type);
            issueFlagSpan.textContent = txn.issue_flag;
            issueFlagCell.appendChild(issueFlagSpan);

            const detailsCell = row.insertCell();
            const detailsButton = document.createElement('button');
            detailsButton.textContent = 'View Details';
            detailsButton.onclick = () => showTransactionDetails(txn.transaction_id);
            detailsCell.appendChild(detailsButton);
        });

    } catch (error) {
        console.error('Error loading transactions:', error);
    }
}

async function showTransactionDetails(transactionId) {
    const modal = document.getElementById('transactionDetailModal');
    const detailContent = document.getElementById('detailContent');
    detailContent.innerHTML = 'Loading...';
    modal.style.display = 'block';

    try {
        const response = await fetch(`${API_BASE_URL}/transactions/${transactionId}`);
        const txn = await response.json();

        if (response.ok) {
            let settlementHistoryHtml = '<h3>Settlement History</h3>';
            if (txn.settlement_history && txn.settlement_history.length > 0) {
                settlementHistoryHtml += '<table><thead><tr><th>Settlement ID</th><th>Date</th><th>Amount</th><th>Type</th></tr></thead><tbody>';
                txn.settlement_history.forEach(sh => {
                    settlementHistoryHtml += `<tr><td>${sh.settlement_id}</td><td>${sh.settlement_date}</td><td>$${sh.settlement_amount.toFixed(2)}</td><td>${sh.settlement_type}</td></tr>`;
                });
                settlementHistoryHtml += '</tbody></table>';
            } else {
                settlementHistoryHtml += '<p>No settlement history found for this transaction.</p>';
            }

            let issueDetailsHtml = '<h3>Issue Details & Recommendations</h3>';
            if (txn.issue_details && txn.issue_details.length > 0) {
                issueDetailsHtml += '<ul>';
                txn.issue_details.forEach(issue => {
                    issueDetailsHtml += `<li>${issue}</li>`;
                });
                issueDetailsHtml += '</ul>';
            } else {
                issueDetailsHtml += '<p>No specific issues detected for this transaction.</p>';
            }

            detailContent.innerHTML = `
                <p><strong>Transaction ID:</strong> ${txn.transaction_id}</p>
                <p><strong>Lifecycle ID:</strong> ${txn.lifecycle_id || 'N/A'}</p>
                <p><strong>Account ID:</strong> ${txn.account_id}</p>
                <p><strong>Merchant:</strong> ${txn.merchant_name}</p>
                <p><strong>Transaction Date:</strong> ${txn.transaction_date}</p>
                <p><strong>Transaction Amount:</strong> $${txn.transaction_amount.toFixed(2)}</p>
                <p><strong>Currency:</strong> ${txn.currency}</p>
                <p><strong>Status:</strong> ${txn.status}</p>
                <p><strong>Settlement Status:</strong> <span class="status-badge status-${txn.settlement_status.replace(/\s/g, '_')}">${txn.settlement_status}</span></p>
                <p><strong>Total Settled Amount:</strong> $${txn.total_settled_amount.toFixed(2)}</p>
                <p><strong>Last Settlement Date:</strong> ${txn.last_settlement_date || 'N/A'}</p>
                <p><strong>Issue Flag:</strong> <span class="issue-flag ${txn.issue_type}">${txn.issue_details.join(', ') || 'No Issues'}</span></p>
                ${settlementHistoryHtml}
                ${issueDetailsHtml}
            `;
        } else {
            detailContent.innerHTML = `<p>Error: ${txn.error}</p>`;
        }
    } catch (error) {
        console.error('Error fetching transaction details:', error);
        detailContent.innerHTML = '<p>An error occurred while loading details.</p>';
    }
}
