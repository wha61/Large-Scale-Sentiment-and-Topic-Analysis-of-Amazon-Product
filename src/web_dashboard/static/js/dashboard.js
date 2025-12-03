// Dashboard JavaScript for Amazon Reviews Analysis

// Global variables
let ratingData = [];
let mismatchedData = [];
let stats = {};
let temporalData = {};
let dataProcessingStats = {};

// Chart instances
let charts = {};

// Initialize dashboard
document.addEventListener('DOMContentLoaded', function() {
    showLoadingState();
    loadAllData();
});

// Show loading state
function showLoadingState() {
    const container = document.querySelector('.container');
    if (container) {
        container.style.opacity = '0.6';
        container.style.pointerEvents = 'none';
    }
}

// Hide loading state
function hideLoadingState() {
    const container = document.querySelector('.container');
    if (container) {
        container.style.opacity = '1';
        container.style.pointerEvents = 'auto';
    }
}

// Load all data from API
async function loadAllData() {
    try {
        // Load statistics
        const statsResponse = await fetch('/api/stats');
        stats = await statsResponse.json();
        updateStats();

        // Load rating vs sentiment data
        const ratingResponse = await fetch('/api/rating-vs-sentiment');
        ratingData = await ratingResponse.json();
        renderRatingCharts();

        // Load mismatched reviews
        const mismatchedResponse = await fetch('/api/mismatched-reviews');
        mismatchedData = await mismatchedResponse.json();
        renderMismatchedCharts();
        renderMismatchedTable(mismatchedData);
        renderReviewExamples(mismatchedData);
        setupCarouselHover();

        // Load temporal trends
        try {
            const temporalResponse = await fetch('/api/temporal-trends');
            temporalData = await temporalResponse.json();
            renderTemporalCharts();
        } catch (e) {
            console.warn('Temporal data not available:', e);
        }

        // Load data processing statistics
        try {
            const processingStatsResponse = await fetch('/api/data-processing-stats');
            if (!processingStatsResponse.ok) {
                console.error('Failed to load data processing stats:', processingStatsResponse.status);
                const errorData = await processingStatsResponse.json();
                console.error('Error details:', errorData);
            } else {
                dataProcessingStats = await processingStatsResponse.json();
                console.log('Data processing stats loaded:', dataProcessingStats);
                renderDataProcessingStats();
            }
        } catch (e) {
            console.error('Error loading data processing stats:', e);
        }

    } catch (error) {
        console.error('Error loading data:', error);
        hideLoadingState();
        showError('Failed to load data. Please check if the server is running and data files exist.', error);
    } finally {
        hideLoadingState();
    }
}

// Show error message
function showError(message, error = null) {
    const errorDiv = document.createElement('div');
    errorDiv.className = 'error-message';
    errorDiv.style.cssText = 'background: #fee; border: 1px solid #fcc; color: #c33; padding: 15px; margin: 20px; border-radius: 6px;';
    errorDiv.innerHTML = `
        <strong>Error:</strong> ${message}
        ${error ? `<br><small>${error.message || error}</small>` : ''}
    `;
    document.querySelector('.container').insertBefore(errorDiv, document.querySelector('.container').firstChild);
}

// Update statistics cards with consistent formatting
function updateStats() {
    const formatNumber = (num) => {
        if (num === null || num === undefined) return '0';
        return typeof num === 'number' ? num.toLocaleString() : num;
    };
    
    document.getElementById('totalRatings').textContent = formatNumber(stats.total_ratings);
    document.getElementById('mismatchedCount').textContent = formatNumber(stats.mismatched_count);
    document.getElementById('avgMAE').textContent = stats.avg_mae ? stats.avg_mae.toFixed(3) : '0.000';
}

// Show stat details modal
function showStatDetails(type) {
    const modal = document.getElementById('statModal');
    const modalTitle = document.getElementById('modalTitle');
    const modalBody = document.getElementById('modalBody');
    
    // Remove active class from all cards
    document.querySelectorAll('.stat-card').forEach(card => card.classList.remove('active'));
    // Add active class to clicked card
    event.currentTarget.classList.add('active');
    
    let title = '';
    let content = '';
    
    switch(type) {
        case 'total':
            title = 'Total Reviews Details';
            content = `
                <p><strong>Total Reviews:</strong> ${stats.total_ratings?.toLocaleString() || '0'}</p>
                <p>This represents the complete dataset of Amazon product reviews analyzed in this study.</p>
                <p>The analysis covers reviews from the All Beauty category, spanning multiple years of consumer feedback.</p>
            `;
            break;
        case 'mismatched':
            title = 'Mismatched Reviews Details';
            content = `
                <p><strong>Mismatched Reviews:</strong> ${stats.mismatched_count?.toLocaleString() || '0'}</p>
                <p>These are reviews where the user's star rating differs significantly from the AI-generated sentiment score.</p>
                <p>Mismatches can indicate:</p>
                <ul>
                    <li>Sarcastic or ironic reviews</li>
                    <li>Potential fake reviews</li>
                    <li>Context-dependent sentiment not captured by the model</li>
                </ul>
            `;
            break;
        case 'mae':
            title = 'Mean Absolute Error (MAE)';
            content = `
                <p><strong>Average MAE:</strong> ${stats.avg_mae?.toFixed(3) || '0.000'}</p>
                <p style="margin-top: 15px;"><strong>Definition:</strong> MAE measures the average absolute difference between user ratings (1-5 stars) and AI-generated sentiment scores (1-5 stars).</p>
                <p style="margin-top: 10px;"><strong>Formula:</strong> MAE = Average( |rating - sentiment_star| )</p>
                <p style="margin-top: 10px;"><strong>Interpretation:</strong> A lower MAE indicates better alignment between human ratings and automated sentiment analysis. For example, an MAE of 0.5 means that on average, the sentiment score differs from the user rating by 0.5 stars.</p>
                <p style="margin-top: 15px; padding-top: 15px; border-top: 1px solid #e1e8ed;"><strong>Reference Standards (1-5 star rating system):</strong></p>
                <ul style="margin-top: 8px; padding-left: 20px; line-height: 1.8;">
                    <li><strong>MAE &lt; 0.5:</strong> Excellent alignment - ratings and sentiment are highly consistent</li>
                    <li><strong>MAE 0.5-1.0:</strong> Good alignment - minor differences, acceptable variance</li>
                    <li><strong>MAE 1.0-2.0:</strong> Moderate differences - noticeable discrepancies exist</li>
                    <li><strong>MAE 2.0-3.0:</strong> Significant differences - warrants further investigation</li>
                    <li><strong>MAE ≥ 3.0:</strong> Severe mismatch - indicates potential fake reviews or model errors (project threshold)</li>
                </ul>
            `;
            break;
    }
    
    modalTitle.textContent = title;
    modalBody.innerHTML = content;
    modal.style.display = 'block';
}

// Close modal
function closeModal() {
    document.getElementById('statModal').style.display = 'none';
    document.querySelectorAll('.stat-card').forEach(card => card.classList.remove('active'));
}

// Close modal when clicking outside
window.onclick = function(event) {
    const modal = document.getElementById('statModal');
    if (event.target == modal) {
        closeModal();
    }
}

// Tab switching functions
// Render temporal trend charts
function renderTemporalCharts() {
    renderYearlyChart();
    renderYearlyTable();
}

function renderYearlyChart() {
    const ctx = document.getElementById('yearlyTrendChart');
    if (!ctx || !temporalData.yearly || temporalData.yearly.length === 0) return;
    
    if (charts.yearly) charts.yearly.destroy();
    
    const data = temporalData.yearly.filter(d => d.year); // Filter out null years
    charts.yearly = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.map(d => d.year),
            datasets: [{
                label: 'Average Sentiment',
                data: data.map(d => d.avg_sentiment),
                borderColor: 'rgba(66, 153, 225, 1)',
                backgroundColor: 'rgba(66, 153, 225, 0.1)',
                borderWidth: 2,
                fill: true,
                tension: 0.4,
                yAxisID: 'y'
            }, {
                label: 'Average Rating',
                data: data.map(d => d.avg_rating),
                borderColor: 'rgba(237, 137, 54, 1)',
                backgroundColor: 'rgba(237, 137, 54, 0.1)',
                borderWidth: 2,
                fill: true,
                tension: 0.4,
                yAxisID: 'y'
            }, {
                label: 'Inflation Rate (%)',
                data: data.map(d => d.inflation_rate !== null && d.inflation_rate !== undefined ? d.inflation_rate : null),
                borderColor: 'rgba(239, 68, 68, 1)',
                backgroundColor: 'rgba(239, 68, 68, 0.1)',
                borderWidth: 2,
                borderDash: [5, 5],
                fill: false,
                tension: 0.4,
                yAxisID: 'y1',
                pointStyle: 'cross',
                pointRadius: 4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                title: {
                    display: true,
                    text: 'Yearly Sentiment, Rating, and Inflation Trends'
                }
            },
            scales: {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    beginAtZero: false,
                    min: 3.5,
                    max: 5,
                    title: {
                        display: true,
                        text: 'Sentiment / Rating (1-5)'
                    }
                },
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Inflation Rate (%)'
                    },
                    grid: {
                        drawOnChartArea: false
                    }
                }
            }
        }
    });
}

function renderYearlyTable() {
    const tbody = document.getElementById('yearlyStatsTableBody');
    if (!tbody || !temporalData.yearly || temporalData.yearly.length === 0) return;
    
    tbody.innerHTML = '';
    
    temporalData.yearly.forEach(d => {
        const row = tbody.insertRow();
        row.insertCell(0).textContent = d.year || 'N/A';
        row.insertCell(1).textContent = d.avg_sentiment ? d.avg_sentiment.toFixed(2) : 'N/A';
        row.insertCell(2).textContent = d.avg_rating ? d.avg_rating.toFixed(2) : 'N/A';
        row.insertCell(3).textContent = d.inflation_rate !== null && d.inflation_rate !== undefined ? d.inflation_rate.toFixed(2) + '%' : 'N/A';
        row.insertCell(4).textContent = d.count ? d.count.toLocaleString() : '0';
    });
}

// Render distribution charts
// Render anomaly detection charts
// Render rating vs sentiment charts
function renderRatingCharts() {
    // Chart 1: Rating vs Average Sentiment
    const ctx1 = document.getElementById('ratingVsSentimentChart').getContext('2d');
    if (charts.ratingVsSentiment) charts.ratingVsSentiment.destroy();
    charts.ratingVsSentiment = new Chart(ctx1, {
        type: 'bar',
        data: {
            labels: ratingData.map(d => `Rating ${d.rating}`),
            datasets: [{
                label: 'Average Sentiment Star',
                data: ratingData.map(d => d.avg_sentiment_star),
                backgroundColor: 'rgba(66, 153, 225, 0.6)',
                borderColor: 'rgba(66, 153, 225, 1)',
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                title: {
                    display: true,
                    text: 'Average Sentiment Score by User Rating'
                },
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    max: 5
                }
            }
        }
    });

    // Chart 2: Rating Distribution
    const ctx2 = document.getElementById('ratingDistributionChart').getContext('2d');
    if (charts.ratingDist) charts.ratingDist.destroy();
    charts.ratingDist = new Chart(ctx2, {
        type: 'doughnut',
        data: {
            labels: ratingData.map(d => `Rating ${d.rating}`),
            datasets: [{
                data: ratingData.map(d => d.count),
                backgroundColor: [
                    'rgba(239, 68, 68, 0.6)',
                    'rgba(245, 158, 11, 0.6)',
                    'rgba(251, 191, 36, 0.6)',
                    'rgba(34, 197, 94, 0.6)',
                    'rgba(59, 130, 246, 0.6)'
                ],
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                title: {
                    display: true,
                    text: 'Review Count Distribution by Rating'
                },
                legend: {
                    position: 'right'
                }
            }
        }
    });

    // Chart 3: MAE by Rating
    const ctx3 = document.getElementById('maeByRatingChart').getContext('2d');
    if (charts.maeByRating) charts.maeByRating.destroy();
    charts.maeByRating = new Chart(ctx3, {
        type: 'line',
        data: {
            labels: ratingData.map(d => `Rating ${d.rating}`),
            datasets: [{
                label: 'Mean Absolute Error',
                data: ratingData.map(d => d.avg_abs_diff),
                borderColor: 'rgba(139, 92, 246, 1)',
                backgroundColor: 'rgba(139, 92, 246, 0.1)',
                borderWidth: 3,
                fill: true,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                title: {
                    display: true,
                    text: 'Mean Absolute Error by Rating'
                }
            },
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}

// Render topic modeling charts
// Render mismatched reviews charts
function renderMismatchedCharts() {
    // Chart 1: Mismatch Distribution
    const mismatchCounts = {};
    mismatchedData.forEach(d => {
        const diff = Math.abs(d.diff);
        mismatchCounts[diff] = (mismatchCounts[diff] || 0) + 1;
    });

    const ctx1 = document.getElementById('mismatchDistributionChart').getContext('2d');
    if (charts.mismatchDist) charts.mismatchDist.destroy();
    charts.mismatchDist = new Chart(ctx1, {
        type: 'bar',
        data: {
            labels: Object.keys(mismatchCounts).sort((a, b) => a - b).map(k => `Diff: ${k}`),
            datasets: [{
                label: 'Count',
                data: Object.keys(mismatchCounts).sort((a, b) => a - b).map(k => mismatchCounts[k]),
                backgroundColor: 'rgba(239, 68, 68, 0.6)',
                borderColor: 'rgba(239, 68, 68, 1)',
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                title: {
                    display: true,
                    text: 'Mismatch Distribution (Rating - Sentiment)'
                },
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}

// Render topics table with theme classification
// Render mismatched reviews table with enhanced features
let allTableData = [];
let currentSortColumn = null;
let currentSortDirection = 'asc';

function renderMismatchedTable(data) {
    allTableData = data;
    const tbody = document.getElementById('reviewsTableBody');
    tbody.innerHTML = '';

    data.forEach((review, index) => {
        const row = tbody.insertRow();
        row.setAttribute('data-index', index);
        
        // Rating cell
        const ratingCell = row.insertCell(0);
        ratingCell.textContent = review.rating;
        ratingCell.setAttribute('data-value', review.rating);
        
        // Sentiment cell
        const sentimentCell = row.insertCell(1);
        sentimentCell.textContent = review.sentiment_star;
        sentimentCell.setAttribute('data-value', review.sentiment_star);
        
        // Confidence cell
        const confCell = row.insertCell(2);
        const conf = parseFloat(review.sentiment_conf);
        confCell.textContent = conf.toFixed(3);
        confCell.setAttribute('data-value', conf);
        if (conf < 0.4) confCell.className = 'badge badge-low';
        else if (conf < 0.7) confCell.className = 'badge badge-medium';
        else confCell.className = 'badge badge-high';
        
        // Difference cell
        const diffCell = row.insertCell(3);
        const diff = parseFloat(review.diff || review.abs_diff || 0);
        diffCell.textContent = diff.toFixed(1);
        diffCell.setAttribute('data-value', Math.abs(diff));
        
        // Text cell
        const textCell = row.insertCell(4);
        textCell.className = 'review-text';
        textCell.textContent = review.text || 'N/A';
    });

    // Add search functionality with multi-field support
    const searchInput = document.getElementById('reviewSearch');
    if (searchInput) {
        // Remove existing listeners
        const newSearchInput = searchInput.cloneNode(true);
        searchInput.parentNode.replaceChild(newSearchInput, searchInput);
        
        newSearchInput.addEventListener('input', function(e) {
            const searchTerm = e.target.value.toLowerCase().trim();
            const rows = tbody.getElementsByTagName('tr');
            let visibleCount = 0;
            
            Array.from(rows).forEach(row => {
                const rating = row.cells[0].textContent;
                const sentiment = row.cells[1].textContent;
                const diff = row.cells[3].textContent;
                const text = row.cells[4].textContent.toLowerCase();
                
                const matches = !searchTerm || 
                    text.includes(searchTerm) ||
                    rating.includes(searchTerm) ||
                    sentiment.includes(searchTerm) ||
                    diff.includes(searchTerm);
                
                row.style.display = matches ? '' : 'none';
                if (matches) visibleCount++;
            });
            
            // Show result count
            updateSearchResultCount(visibleCount, rows.length);
        });
    }
    
    // Add sort functionality to table headers
    addTableSorting();
}

// Add sorting to table headers
function addTableSorting() {
    const headers = document.querySelectorAll('#reviewsTable thead th');
    headers.forEach((header, index) => {
        if (index < 4) { // Only make first 4 columns sortable (not text column)
            header.style.cursor = 'pointer';
            header.style.userSelect = 'none';
            header.innerHTML += ' <span class="sort-indicator">↕</span>';
            header.addEventListener('click', () => sortTable(index));
        }
    });
}

// Sort table by column
function sortTable(columnIndex) {
    const tbody = document.getElementById('reviewsTableBody');
    const rows = Array.from(tbody.getElementsByTagName('tr'));
    
    // Toggle sort direction if clicking same column
    if (currentSortColumn === columnIndex) {
        currentSortDirection = currentSortDirection === 'asc' ? 'desc' : 'asc';
    } else {
        currentSortColumn = columnIndex;
        currentSortDirection = 'asc';
    }
    
    // Sort rows
    rows.sort((a, b) => {
        const aValue = parseFloat(a.cells[columnIndex].getAttribute('data-value') || a.cells[columnIndex].textContent) || 0;
        const bValue = parseFloat(b.cells[columnIndex].getAttribute('data-value') || b.cells[columnIndex].textContent) || 0;
        
        if (currentSortDirection === 'asc') {
            return aValue - bValue;
        } else {
            return bValue - aValue;
        }
    });
    
    // Re-append sorted rows
    rows.forEach(row => tbody.appendChild(row));
    
    // Update sort indicators
    updateSortIndicators(columnIndex);
}

// Update sort indicators in headers
function updateSortIndicators(activeColumn) {
    const headers = document.querySelectorAll('#reviewsTable thead th');
    headers.forEach((header, index) => {
        const indicator = header.querySelector('.sort-indicator');
        if (indicator) {
            if (index === activeColumn) {
                indicator.textContent = currentSortDirection === 'asc' ? ' ↑' : ' ↓';
            } else {
                indicator.textContent = ' ↕';
            }
        }
    });
}

// Update search result count
function updateSearchResultCount(visible, total) {
    let countDiv = document.getElementById('searchResultCount');
    if (!countDiv) {
        countDiv = document.createElement('div');
        countDiv.id = 'searchResultCount';
        countDiv.style.cssText = 'margin: 10px 0; color: #666; font-size: 0.9em;';
        const searchBox = document.querySelector('.search-box');
        if (searchBox) {
            searchBox.appendChild(countDiv);
        }
    }
    countDiv.textContent = `Showing ${visible} of ${total} reviews`;
}

// Render data processing statistics
function renderDataProcessingStats() {
    if (!dataProcessingStats || Object.keys(dataProcessingStats).length === 0) {
        console.warn('No data processing stats available');
        console.log('dataProcessingStats:', dataProcessingStats);
        return;
    }

    console.log('Rendering data processing stats:', dataProcessingStats);

    // Format numbers with commas
    function formatNumber(num) {
        if (num === null || num === undefined || num === 0 || num === '-') return '-';
        return num.toLocaleString();
    }

    // Update raw data stats
    const raw = dataProcessingStats.raw || {};
    const rawCountEl = document.getElementById('rawCount');
    const rawSizeEl = document.getElementById('rawSize');
    const rawPartitionsEl = document.getElementById('rawPartitions');
    if (rawCountEl) rawCountEl.textContent = formatNumber(raw.count);
    if (rawSizeEl) rawSizeEl.textContent = raw.size_mb || '-';
    if (rawPartitionsEl) rawPartitionsEl.textContent = raw.partitions || '-';

    // Update cleaned data stats
    const cleaned = dataProcessingStats.cleaned || {};
    const cleanedCountEl = document.getElementById('cleanedCount');
    const cleanedSizeEl = document.getElementById('cleanedSize');
    const cleanedPartitionsEl = document.getElementById('cleanedPartitions');
    if (cleanedCountEl) cleanedCountEl.textContent = formatNumber(cleaned.count);
    if (cleanedSizeEl) cleanedSizeEl.textContent = cleaned.size_mb || '-';
    if (cleanedPartitionsEl) cleanedPartitionsEl.textContent = cleaned.partitions || '-';

    // Sentiment Analysis stats removed - only showing Raw and Cleaned data

    // Update processing metrics
    const metrics = dataProcessingStats.processing_metrics || {};
    const retentionRateEl = document.getElementById('retentionRate');
    const dataReductionEl = document.getElementById('dataReduction');
    if (retentionRateEl) retentionRateEl.textContent = metrics.retention_rate ? metrics.retention_rate + '%' : '-';
    if (dataReductionEl) dataReductionEl.textContent = metrics.data_reduction ? metrics.data_reduction + '%' : '-';

    // Update Spark info
    const sparkInfo = dataProcessingStats.spark_info || {};
    const sparkAppNameEl = document.getElementById('sparkAppName');
    const sparkVersionEl = document.getElementById('sparkVersion');
    const sparkParallelismEl = document.getElementById('sparkParallelism');
    if (sparkAppNameEl) sparkAppNameEl.textContent = sparkInfo.app_name || '-';
    if (sparkVersionEl) sparkVersionEl.textContent = sparkInfo.spark_version || '-';
    if (sparkParallelismEl) sparkParallelismEl.textContent = sparkInfo.default_parallelism || '-';

    // Update data quality metrics
    const quality = dataProcessingStats.data_quality || {};
    const dataCompletenessEl = document.getElementById('dataCompleteness');
    const nullTextEl = document.getElementById('nullText');
    const nullRatingEl = document.getElementById('nullRating');
    const nullSentimentEl = document.getElementById('nullSentiment');
    if (dataCompletenessEl) dataCompletenessEl.textContent = (quality.completeness !== undefined && quality.completeness !== null) ? quality.completeness : '-';
    if (nullTextEl) nullTextEl.textContent = formatNumber(quality.null_text || 0);
    if (nullRatingEl) nullRatingEl.textContent = formatNumber(quality.null_rating || 0);
    if (nullSentimentEl) nullSentimentEl.textContent = formatNumber(quality.null_sentiment || 0);

    // Render data processing flow chart
    renderDataProcessingChart();
}

// Render data processing flow chart
function renderDataProcessingChart() {
    const ctx = document.getElementById('dataProcessingChart');
    if (!ctx) return;

    // Destroy existing chart if it exists
    if (charts.dataProcessingChart) {
        charts.dataProcessingChart.destroy();
    }

    const raw = dataProcessingStats.raw || {};
    const cleaned = dataProcessingStats.cleaned || {};

    charts.dataProcessingChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['Raw Data', 'Cleaned Data'],
            datasets: [
                {
                    label: 'Record Count',
                    data: [raw.count || 0, cleaned.count || 0],
                    backgroundColor: ['rgba(54, 162, 235, 0.6)', 'rgba(75, 192, 192, 0.6)'],
                    borderColor: ['rgba(54, 162, 235, 1)', 'rgba(75, 192, 192, 1)'],
                    borderWidth: 2,
                    yAxisID: 'y'
                },
                {
                    label: 'Data Size (MB)',
                    data: [raw.size_mb || 0, cleaned.size_mb || 0],
                    backgroundColor: ['rgba(255, 99, 132, 0.6)', 'rgba(255, 159, 64, 0.6)'],
                    borderColor: ['rgba(255, 99, 132, 1)', 'rgba(255, 159, 64, 1)'],
                    borderWidth: 2,
                    yAxisID: 'y1'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                title: {
                    display: true,
                    text: 'Data Processing Pipeline: Record Count and Data Size Across Stages',
                    font: {
                        size: 16,
                        weight: 'bold'
                    }
                },
                legend: {
                    display: true,
                    position: 'top'
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            let label = context.dataset.label || '';
                            if (label) {
                                label += ': ';
                            }
                            if (context.dataset.label === 'Record Count') {
                                label += context.parsed.y.toLocaleString() + ' records';
                            } else {
                                label += context.parsed.y.toFixed(2) + ' MB';
                            }
                            return label;
                        }
                    }
                }
            },
            scales: {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    title: {
                        display: true,
                        text: 'Record Count'
                    },
                    ticks: {
                        callback: function(value) {
                            return value.toLocaleString();
                        }
                    }
                },
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    title: {
                        display: true,
                        text: 'Data Size (MB)'
                    },
                    grid: {
                        drawOnChartArea: false
                    }
                }
            }
        }
    });
}

// Review Examples Carousel Functions
let currentReviewIndex = 0;
let totalReviews = 0;

// Render review examples from actual data
function renderReviewExamples(data) {
    if (!data || data.length === 0) {
        console.warn('No mismatched reviews data available');
        return;
    }
    
    // Take up to 20 reviews with highest mismatch
    const topReviews = data
        .filter(r => r.text && r.text.trim().length > 0) // Filter out empty reviews
        .sort((a, b) => Math.abs(parseFloat(b.abs_diff || b.diff || 0)) - Math.abs(parseFloat(a.abs_diff || a.diff || 0)))
        .slice(0, 20);
    
    totalReviews = topReviews.length;
    
    if (totalReviews === 0) {
        console.warn('No valid reviews to display');
        return;
    }
    
    const carousel = document.getElementById('reviewCarousel');
    const dotsContainer = document.getElementById('carouselDots');
    
    if (!carousel || !dotsContainer) {
        console.error('Carousel elements not found');
        return;
    }
    
    // Clear existing content
    carousel.innerHTML = '';
    dotsContainer.innerHTML = '';
    
    // Generate review cards
    topReviews.forEach((review, index) => {
        const rating = parseInt(review.rating) || 0;
        const sentiment = parseFloat(review.sentiment_star) || 0;
        const absDiff = Math.abs(parseFloat(review.abs_diff || review.diff || 0));
        const text = (review.text || '').trim();
        
        if (!text) return;
        
        // Create card
        const card = document.createElement('div');
        card.className = `review-example-card ${index === 0 ? 'active' : ''}`;
        card.setAttribute('data-index', index);
        
        // Generate problem description
        let problemText = '';
        if (rating > sentiment) {
            problemText = `User gave ${rating} stars but the text expresses ${sentiment < 2.5 ? 'negative' : sentiment < 3.5 ? 'neutral' : 'positive'} sentiment (${sentiment.toFixed(1)} stars). This mismatch indicates potential fake review or rating error.`;
        } else if (rating < sentiment) {
            problemText = `User gave ${rating} stars but the text expresses ${sentiment > 3.5 ? 'positive' : sentiment > 2.5 ? 'neutral' : 'negative'} sentiment (${sentiment.toFixed(1)} stars). Rating may not reflect actual product sentiment.`;
        } else {
            problemText = `Rating and sentiment are aligned, but this review was flagged for further analysis.`;
        }
        
        // Escape HTML to prevent XSS
        const escapeHtml = (str) => {
            const div = document.createElement('div');
            div.textContent = str;
            return div.innerHTML;
        };
        
        const escapedText = escapeHtml(text);
        const escapedProblem = escapeHtml(problemText);
        
        card.innerHTML = `
            <div class="review-header">
                <div class="rating-display">
                    <span class="user-rating">User Rating: <span class="rating-value">${rating}</span> ⭐</span>
                    <span class="sentiment-rating">AI Sentiment: <span class="sentiment-value">${sentiment.toFixed(1)}</span> ⭐</span>
                    <span class="mismatch-badge">Mismatch: ${absDiff.toFixed(1)} stars</span>
                </div>
            </div>
            <div class="review-text">
                <p class="review-quote"><strong>Review:</strong> "${escapedText}"</p>
                <p class="review-analysis"><strong>Problem:</strong> ${escapedProblem}</p>
            </div>
        `;
        
        carousel.appendChild(card);
        
        // Create dot
        const dot = document.createElement('span');
        dot.className = `dot ${index === 0 ? 'active' : ''}`;
        dot.setAttribute('onclick', `goToReviewExample(${index})`);
        dotsContainer.appendChild(dot);
    });
    
    // Initialize carousel
    if (totalReviews > 0) {
        goToReviewExample(0);
        
        // Restart auto-rotate
        if (carouselInterval) {
            clearInterval(carouselInterval);
        }
        carouselInterval = setInterval(() => {
            changeReviewExample(1);
        }, 6000); // Changed from 30s to 6s for better UX
    }
}

function changeReviewExample(direction) {
    if (totalReviews === 0) return;
    const newIndex = (currentReviewIndex + direction + totalReviews) % totalReviews;
    goToReviewExample(newIndex);
}

function goToReviewExample(index) {
    // Remove active class from current card and dot
    const currentCard = document.querySelector('.review-example-card.active');
    const currentDot = document.querySelector('.dot.active');
    
    if (currentCard) {
        currentCard.classList.remove('active');
    }
    if (currentDot) {
        currentDot.classList.remove('active');
    }
    
    // Add active class to new card and dot
    const newCard = document.querySelector(`.review-example-card[data-index="${index}"]`);
    const newDot = document.querySelector(`.dot:nth-child(${index + 1})`);
    
    if (newCard) {
        newCard.classList.add('active');
    }
    if (newDot) {
        newDot.classList.add('active');
    }
    
    currentReviewIndex = index;
}

// Auto-rotate carousel every 5 seconds
let carouselInterval;

// Setup carousel hover pause (will be called after reviews are loaded)
function setupCarouselHover() {
    const carouselContainer = document.querySelector('.review-examples-container');
    if (carouselContainer) {
        carouselContainer.addEventListener('mouseenter', () => {
            if (carouselInterval) {
                clearInterval(carouselInterval);
            }
        });
        
        carouselContainer.addEventListener('mouseleave', () => {
            if (totalReviews > 0) {
                carouselInterval = setInterval(() => {
                    changeReviewExample(1);
                }, 6000); // Changed from 30s to 6s
            }
        });
    }
    
    // Add keyboard navigation for carousel
    document.addEventListener('keydown', (e) => {
        // Only handle if not typing in input fields
        if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') {
            return;
        }
        
        if (e.key === 'ArrowLeft') {
            e.preventDefault();
            changeReviewExample(-1);
        } else if (e.key === 'ArrowRight') {
            e.preventDefault();
            changeReviewExample(1);
        }
    });
}
