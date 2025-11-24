// Dashboard JavaScript for Amazon Reviews Analysis

// Global variables
let ratingData = [];
let mismatchedData = [];
let topicData = [];
let stats = {};

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

// Render rating vs sentiment charts
function renderRatingCharts() {
    // Chart 1: Rating vs Average Sentiment
    const ctx1 = document.getElementById('ratingVsSentimentChart').getContext('2d');
    new Chart(ctx1, {
        type: 'bar',
        data: {
            labels: ratingData.map(d => `Rating ${d.rating}`),
            datasets: [{
                label: 'Average Sentiment Star',
                data: ratingData.map(d => d.avg_sentiment_star),
                backgroundColor: 'rgba(102, 126, 234, 0.6)',
                borderColor: 'rgba(102, 126, 234, 1)',
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
    new Chart(ctx2, {
        type: 'doughnut',
        data: {
            labels: ratingData.map(d => `Rating ${d.rating}`),
            datasets: [{
                data: ratingData.map(d => d.count),
                backgroundColor: [
                    'rgba(255, 99, 132, 0.6)',
                    'rgba(255, 159, 64, 0.6)',
                    'rgba(255, 205, 86, 0.6)',
                    'rgba(75, 192, 192, 0.6)',
                    'rgba(54, 162, 235, 0.6)'
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
    new Chart(ctx3, {
        type: 'line',
        data: {
            labels: ratingData.map(d => `Rating ${d.rating}`),
            datasets: [{
                label: 'Mean Absolute Error',
                data: ratingData.map(d => d.avg_abs_diff),
                borderColor: 'rgba(118, 75, 162, 1)',
                backgroundColor: 'rgba(118, 75, 162, 0.1)',
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
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: topicData.map(d => `Topic ${d.Topic}`),
            datasets: [{
                label: 'Number of Reviews',
                data: topicData.map(d => d.Count),
                backgroundColor: 'rgba(102, 126, 234, 0.6)',
                borderColor: 'rgba(102, 126, 234, 1)',
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
    new Chart(ctx1, {
        type: 'bar',
        data: {
            labels: Object.keys(mismatchCounts).sort((a, b) => a - b).map(k => `Diff: ${k}`),
            datasets: [{
                label: 'Count',
                data: Object.keys(mismatchCounts).sort((a, b) => a - b).map(k => mismatchCounts[k]),
                backgroundColor: 'rgba(255, 99, 132, 0.6)',
                borderColor: 'rgba(255, 99, 132, 1)',
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
    new Chart(ctx2, {
        type: 'pie',
        data: {
            labels: Object.keys(confidenceRanges),
            datasets: [{
                data: Object.values(confidenceRanges),
                backgroundColor: [
                    'rgba(255, 99, 132, 0.6)',
                    'rgba(255, 159, 64, 0.6)',
                    'rgba(255, 205, 86, 0.6)',
                    'rgba(75, 192, 192, 0.6)',
                    'rgba(54, 162, 235, 0.6)'
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

// Render topics table
function renderTopicsTable(data) {
    const tbody = document.getElementById('topicsTableBody');
    tbody.innerHTML = '';

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
            }
        } catch (e) {
            topWords = topic.Representation || 'N/A';
        }
        row.insertCell(3).textContent = topWords;
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
    searchInput.addEventListener('input', function(e) {
        const searchTerm = e.target.value.toLowerCase();
        const rows = tbody.getElementsByTagName('tr');
        
        Array.from(rows).forEach(row => {
            const text = row.cells[4].textContent.toLowerCase();
            row.style.display = text.includes(searchTerm) ? '' : 'none';
        });
    });
}

