// Dashboard JavaScript for Amazon Reviews Analysis

// Global variables
let ratingData = [];
let mismatchedData = [];
let topicData = [];
let stats = {};
let temporalData = {};
let distributionData = {};
let anomalousUsers = [];

// Chart instances
let charts = {};

// Initialize dashboard
document.addEventListener('DOMContentLoaded', function() {
    loadAllData();
});

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

        // Load topics
        const topicsResponse = await fetch('/api/topics');
        topicData = await topicsResponse.json();
        renderTopicCharts();
        renderTopicsTable(topicData);

        // Load temporal trends
        try {
            const temporalResponse = await fetch('/api/temporal-trends');
            temporalData = await temporalResponse.json();
            renderTemporalCharts();
        } catch (e) {
            console.warn('Temporal data not available:', e);
        }

        // Load distribution data
        try {
            const distResponse = await fetch('/api/sentiment-distribution');
            distributionData = await distResponse.json();
            renderDistributionCharts();
        } catch (e) {
            console.warn('Distribution data not available:', e);
        }

        // Load anomalous users
        try {
            const anomalyResponse = await fetch('/api/anomalous-users');
            anomalousUsers = await anomalyResponse.json();
            renderAnomalyCharts();
        } catch (e) {
            console.warn('Anomaly data not available:', e);
        }

    } catch (error) {
        console.error('Error loading data:', error);
        document.body.innerHTML = '<div class="loading">Error loading data. Please check if the server is running and data files exist.</div>';
    }
}

// Update statistics cards
function updateStats() {
    document.getElementById('totalRatings').textContent = stats.total_ratings?.toLocaleString() || '0';
    document.getElementById('mismatchedCount').textContent = stats.mismatched_count?.toLocaleString() || '0';
    document.getElementById('topicCount').textContent = stats.topic_count || '0';
    document.getElementById('avgMAE').textContent = stats.avg_mae?.toFixed(3) || '0.000';
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
        case 'topics':
            title = 'Topics Identified';
            content = `
                <p><strong>Topics Found:</strong> ${stats.topic_count || '0'}</p>
                <p>Topic modeling using BERTopic has identified distinct themes in the review data.</p>
                <p>Common topics include concerns about quality, durability, fit, and value for money.</p>
            `;
            break;
        case 'mae':
            title = 'Mean Absolute Error (MAE)';
            content = `
                <p><strong>Average MAE:</strong> ${stats.avg_mae?.toFixed(3) || '0.000'}</p>
                <p><strong>Definition:</strong> MAE measures the average absolute difference between user ratings (1-5 stars) and AI-generated sentiment scores (1-5 stars).</p>
                <p><strong>Interpretation:</strong></p>
                <ul>
                    <li>Lower MAE = Better alignment between human ratings and AI sentiment</li>
                    <li>MAE of 0.5 means sentiment differs from rating by 0.5 stars on average</li>
                    <li>This metric helps validate the accuracy of the sentiment analysis model</li>
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
function switchTab(tabName) {
    document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
    
    event.target.classList.add('active');
    document.getElementById(tabName + '-tab').classList.add('active');
    
    if (tabName === 'yearly') {
        renderYearlyChart();
    } else if (tabName === 'monthly') {
        renderMonthlyChart();
    }
}

function switchDistributionTab(tabName) {
    document.querySelectorAll('.tab').forEach(tab => {
        if (tab.closest('.chart-section').querySelector('h2').textContent === 'Sentiment Distribution Analysis') {
            tab.classList.remove('active');
        }
    });
    document.querySelectorAll('.tab-content').forEach(content => {
        if (content.id.includes('dist-tab')) {
            content.classList.remove('active');
        }
    });
    
    event.target.classList.add('active');
    document.getElementById(tabName + '-dist-tab').classList.add('active');
}

function switchAnomalyTab(tabName) {
    document.querySelectorAll('.tab').forEach(tab => {
        if (tab.closest('.chart-section').querySelector('h2').textContent === 'Behavioral Anomaly Detection') {
            tab.classList.remove('active');
        }
    });
    document.querySelectorAll('.tab-content').forEach(content => {
        if (content.id.includes('-tab') && !content.id.includes('dist-tab')) {
            content.classList.remove('active');
        }
    });
    
    event.target.classList.add('active');
    document.getElementById(tabName + '-tab').classList.add('active');
}

// Render temporal trend charts
function renderTemporalCharts() {
    renderYearlyChart();
    renderMonthlyChart();
}

function renderYearlyChart() {
    const ctx = document.getElementById('yearlyTrendChart');
    if (!ctx || !temporalData.yearly || temporalData.yearly.length === 0) return;
    
    if (charts.yearly) charts.yearly.destroy();
    
    const data = temporalData.yearly;
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
                tension: 0.4
            }, {
                label: 'Average Rating',
                data: data.map(d => d.avg_rating),
                borderColor: 'rgba(237, 137, 54, 1)',
                backgroundColor: 'rgba(237, 137, 54, 0.1)',
                borderWidth: 2,
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
                    text: 'Yearly Sentiment and Rating Trends'
                }
            },
            scales: {
                y: {
                    beginAtZero: false,
                    min: 1,
                    max: 5
                }
            }
        }
    });
}

function renderMonthlyChart() {
    const ctx = document.getElementById('monthlyTrendChart');
    if (!ctx || !temporalData.monthly || temporalData.monthly.length === 0) return;
    
    if (charts.monthly) charts.monthly.destroy();
    
    const data = temporalData.monthly;
    charts.monthly = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.map(d => `${d.year}-${String(d.month).padStart(2, '0')}`),
            datasets: [{
                label: 'Average Sentiment',
                data: data.map(d => d.avg_sentiment),
                borderColor: 'rgba(66, 153, 225, 1)',
                backgroundColor: 'rgba(66, 153, 225, 0.1)',
                borderWidth: 2,
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
                    text: 'Monthly Sentiment Trends'
                }
            },
            scales: {
                y: {
                    beginAtZero: false,
                    min: 1,
                    max: 5
                },
                x: {
                    ticks: {
                        maxRotation: 45,
                        minRotation: 45
                    }
                }
            }
        }
    });
}

// Render distribution charts
function renderDistributionCharts() {
    renderProductChart();
    renderCategoryChart();
}

function renderProductChart() {
    const ctx = document.getElementById('productSentimentChart');
    if (!ctx || !distributionData.product || distributionData.product.length === 0) return;
    
    if (charts.product) charts.product.destroy();
    
    const data = distributionData.product.slice(0, 15); // Top 15
    charts.product = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => d.asin.substring(0, 12) + '...'),
            datasets: [{
                label: 'Average Sentiment',
                data: data.map(d => d.avg_sentiment),
                backgroundColor: 'rgba(66, 153, 225, 0.6)',
                borderColor: 'rgba(66, 153, 225, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                title: {
                    display: true,
                    text: 'Sentiment Distribution by Product (Top 15)'
                },
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: false,
                    min: 1,
                    max: 5
                }
            }
        }
    });
}

function renderCategoryChart() {
    const ctx = document.getElementById('categorySentimentChart');
    if (!ctx || !distributionData.category || distributionData.category.length === 0) return;
    
    if (charts.category) charts.category.destroy();
    
    charts.category = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: distributionData.category.map(d => d.category),
            datasets: [{
                label: 'Average Sentiment',
                data: distributionData.category.map(d => d.avg_sentiment),
                backgroundColor: 'rgba(102, 126, 234, 0.6)',
                borderColor: 'rgba(102, 126, 234, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                title: {
                    display: true,
                    text: 'Sentiment Distribution by Category'
                },
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: false,
                    min: 1,
                    max: 5
                }
            }
        }
    });
}

// Render anomaly detection charts
function renderAnomalyCharts() {
    renderSpammerChart();
    renderConflictedChart();
    renderAnomalyTables();
}

function renderSpammerChart() {
    const ctx = document.getElementById('spammerChart');
    if (!ctx || !anomalousUsers || anomalousUsers.length === 0) return;
    
    const spammers = anomalousUsers.filter(u => u.reason && u.reason.includes('Spammer'));
    
    if (charts.spammer) charts.spammer.destroy();
    
    charts.spammer = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: spammers.slice(0, 10).map(u => u.user_id.substring(0, 10) + '...'),
            datasets: [{
                label: 'Review Count',
                data: spammers.slice(0, 10).map(u => u.review_count),
                backgroundColor: 'rgba(239, 68, 68, 0.6)',
                borderColor: 'rgba(239, 68, 68, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                title: {
                    display: true,
                    text: 'High-Frequency Reviewers (Potential Spammers)'
                },
                legend: {
                    display: false
                }
            }
        }
    });
}

function renderConflictedChart() {
    const ctx = document.getElementById('conflictedChart');
    if (!ctx || !anomalousUsers || anomalousUsers.length === 0) return;
    
    const conflicted = anomalousUsers.filter(u => u.reason && u.reason.includes('Conflicted'));
    
    if (charts.conflicted) charts.conflicted.destroy();
    
    charts.conflicted = new Chart(ctx, {
        type: 'scatter',
        data: {
            datasets: [{
                label: 'Conflicted Users',
                data: conflicted.slice(0, 20).map(u => ({
                    x: u.avg_rating,
                    y: u.avg_sentiment
                })),
                backgroundColor: 'rgba(245, 158, 11, 0.6)',
                borderColor: 'rgba(245, 158, 11, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                title: {
                    display: true,
                    text: 'Conflicted Users: High Rating vs Low Sentiment'
                }
            },
            scales: {
                x: {
                    title: {
                        display: true,
                        text: 'Average Rating'
                    },
                    min: 1,
                    max: 5
                },
                y: {
                    title: {
                        display: true,
                        text: 'Average Sentiment'
                    },
                    min: 1,
                    max: 5
                }
            }
        }
    });
}

function renderAnomalyTables() {
    const spammers = anomalousUsers.filter(u => u.reason && u.reason.includes('Spammer'));
    const conflicted = anomalousUsers.filter(u => u.reason && u.reason.includes('Conflicted'));
    
    // Spammers table
    const spammersTbody = document.getElementById('spammersTableBody');
    if (spammersTbody) {
        spammersTbody.innerHTML = '';
        spammers.slice(0, 20).forEach(user => {
            const row = spammersTbody.insertRow();
            row.insertCell(0).textContent = user.user_id?.substring(0, 15) + '...' || 'N/A';
            row.insertCell(1).textContent = user.review_count || '0';
            row.insertCell(2).textContent = (user.avg_rating || 0).toFixed(2);
            row.insertCell(3).textContent = (user.rating_stddev || 0).toFixed(3);
            row.insertCell(4).textContent = user.product_count || '0';
        });
    }
    
    // Conflicted table
    const conflictedTbody = document.getElementById('conflictedTableBody');
    if (conflictedTbody) {
        conflictedTbody.innerHTML = '';
        conflicted.slice(0, 20).forEach(user => {
            const row = conflictedTbody.insertRow();
            row.insertCell(0).textContent = user.user_id?.substring(0, 15) + '...' || 'N/A';
            row.insertCell(1).textContent = user.review_count || '0';
            row.insertCell(2).textContent = (user.avg_rating || 0).toFixed(2);
            row.insertCell(3).textContent = (user.avg_sentiment || 0).toFixed(2);
            row.insertCell(4).textContent = ((user.avg_rating || 0) - (user.avg_sentiment || 0)).toFixed(2);
        });
    }
}

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
function renderTopicCharts() {
    const ctx = document.getElementById('topicDistributionChart').getContext('2d');
    if (charts.topicDist) charts.topicDist.destroy();
    charts.topicDist = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: topicData.map(d => `Topic ${d.Topic}`),
            datasets: [{
                label: 'Number of Reviews',
                data: topicData.map(d => d.Count),
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
                    text: 'Topic Distribution'
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

    // Chart 2: Sentiment Confidence Distribution
    const confidenceRanges = {
        '0.0-0.2': 0,
        '0.2-0.4': 0,
        '0.4-0.6': 0,
        '0.6-0.8': 0,
        '0.8-1.0': 0
    };

    mismatchedData.forEach(d => {
        const conf = d.sentiment_conf;
        if (conf < 0.2) confidenceRanges['0.0-0.2']++;
        else if (conf < 0.4) confidenceRanges['0.2-0.4']++;
        else if (conf < 0.6) confidenceRanges['0.4-0.6']++;
        else if (conf < 0.8) confidenceRanges['0.6-0.8']++;
        else confidenceRanges['0.8-1.0']++;
    });

    const ctx2 = document.getElementById('sentimentConfidenceChart').getContext('2d');
    if (charts.confidenceDist) charts.confidenceDist.destroy();
    charts.confidenceDist = new Chart(ctx2, {
        type: 'pie',
        data: {
            labels: Object.keys(confidenceRanges),
            datasets: [{
                data: Object.values(confidenceRanges),
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
                    text: 'Sentiment Confidence Distribution'
                }
            }
        }
    });
}

// Render topics table with theme classification
function renderTopicsTable(data) {
    const tbody = document.getElementById('topicsTableBody');
    tbody.innerHTML = '';

    // Theme keywords
    const themes = {
        'quality': ['quality', 'durable', 'last', 'break', 'work', 'good', 'bad'],
        'durability': ['last', 'durable', 'long', 'wear', 'hold', 'strong'],
        'fit': ['fit', 'size', 'small', 'large', 'perfect', 'right'],
        'value': ['price', 'worth', 'value', 'cheap', 'expensive', 'money']
    };

    function classifyTheme(name, words) {
        const text = (name + ' ' + words).toLowerCase();
        for (const [theme, keywords] of Object.entries(themes)) {
            if (keywords.some(kw => text.includes(kw))) {
                return theme.charAt(0).toUpperCase() + theme.slice(1);
            }
        }
        return 'Other';
    }

    data.forEach(topic => {
        const row = tbody.insertRow();
        row.insertCell(0).textContent = topic.Topic;
        row.insertCell(1).textContent = topic.Count;
        row.insertCell(2).textContent = topic.Name || 'N/A';
        
        // Parse representation (top words)
        let topWords = 'N/A';
        try {
            if (topic.Representation) {
                const words = JSON.parse(topic.Representation);
                topWords = words.slice(0, 10).join(', ');
                row.insertCell(3).textContent = topWords;
                row.insertCell(4).textContent = classifyTheme(topic.Name || '', topWords);
            } else {
                row.insertCell(3).textContent = 'N/A';
                row.insertCell(4).textContent = 'Other';
            }
        } catch (e) {
            row.insertCell(3).textContent = topic.Representation || 'N/A';
            row.insertCell(4).textContent = 'Other';
        }
    });
}

// Render mismatched reviews table
function renderMismatchedTable(data) {
    const tbody = document.getElementById('reviewsTableBody');
    tbody.innerHTML = '';

    data.forEach(review => {
        const row = tbody.insertRow();
        row.insertCell(0).textContent = review.rating;
        row.insertCell(1).textContent = review.sentiment_star;
        
        const confCell = row.insertCell(2);
        const conf = parseFloat(review.sentiment_conf);
        confCell.textContent = conf.toFixed(3);
        if (conf < 0.4) confCell.className = 'badge badge-low';
        else if (conf < 0.7) confCell.className = 'badge badge-medium';
        else confCell.className = 'badge badge-high';
        
        row.insertCell(3).textContent = review.diff;
        const textCell = row.insertCell(4);
        textCell.className = 'review-text';
        textCell.textContent = review.text || 'N/A';
    });

    // Add search functionality
    const searchInput = document.getElementById('reviewSearch');
    if (searchInput) {
        searchInput.addEventListener('input', function(e) {
            const searchTerm = e.target.value.toLowerCase();
            const rows = tbody.getElementsByTagName('tr');
            
            Array.from(rows).forEach(row => {
                const text = row.cells[4].textContent.toLowerCase();
                row.style.display = text.includes(searchTerm) ? '' : 'none';
            });
        });
    }
}
