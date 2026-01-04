/**
 * DQS Engine Dashboard - JavaScript
 * Handles API communication and UI updates
 */

const API_BASE = '';  // Same origin

// Layer mapping for status updates
const LAYER_MAP = {
    1: { name: 'Input Contract', phase: 1 },
    2: { name: 'Input Validation', phase: 1 },
    3: { name: 'Feature Extraction', phase: 2 },
    4.1: { name: 'Structural Integrity', phase: 3 },
    4.2: { name: 'Field Compliance', phase: 3 },
    4.3: { name: 'Semantic Validation', phase: 3 },
    4.4: { name: 'Anomaly Detection', phase: 4 },
    4.5: { name: 'GenAI Summarization', phase: 4 },
    5: { name: 'Output Contract', phase: 5 },
    6: { name: 'Stability & Consistency', phase: 5 },
    7: { name: 'Conflict Detection', phase: 5 },
    8: { name: 'Confidence Band', phase: 5 },
    9: { name: 'Decision Gate', phase: 6 },
    10: { name: 'Responsibility Boundary', phase: 6 },
    11: { name: 'Logging & Trace', phase: 7 }
};

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    checkHealth();
    resetLayerVisuals();
});

/**
 * Update slider display value
 */
function updateSlider(sliderId, valueId) {
    const slider = document.getElementById(sliderId);
    const valueSpan = document.getElementById(valueId);

    if (sliderId === 'anomalyRate') {
        valueSpan.textContent = slider.value + '%';
    } else {
        valueSpan.textContent = slider.value;
    }
}

/**
 * Check API health
 */
async function checkHealth() {
    try {
        const response = await fetch(`${API_BASE}/api/health`);
        const data = await response.json();

        const statusEl = document.getElementById('apiStatus');
        if (data.status === 'healthy') {
            statusEl.textContent = 'Connected';
            document.querySelector('.status-dot').style.background = '#10b981';
        } else {
            statusEl.textContent = 'Degraded';
            document.querySelector('.status-dot').style.background = '#f59e0b';
        }
    } catch (error) {
        const statusEl = document.getElementById('apiStatus');
        statusEl.textContent = 'Disconnected';
        document.querySelector('.status-dot').style.background = '#ef4444';
    }
}

/**
 * Reset all layer visuals to initial state
 */
function resetLayerVisuals() {
    document.querySelectorAll('.layer').forEach(layer => {
        layer.classList.remove('passed', 'failed', 'degraded', 'running');
    });
}

/**
 * Update layer visual status
 */
function updateLayerStatus(layerId, status) {
    const layer = document.querySelector(`[data-layer="${layerId}"]`);
    if (layer) {
        layer.classList.remove('passed', 'failed', 'degraded', 'running');
        if (status) {
            layer.classList.add(status.toLowerCase());
        }
    }
}

/**
 * Simulate layer execution progress
 */
async function simulateProgress(totalLayers = 15) {
    const layers = [1, 2, 3, 4.1, 4.2, 4.3, 4.4, 4.5, 5, 6, 7, 8, 9, 10, 11];
    const progressBar = document.getElementById('progressBar');
    const loadingLayer = document.getElementById('loadingLayer');

    for (let i = 0; i < layers.length; i++) {
        const layerId = layers[i];
        const layerInfo = LAYER_MAP[layerId];

        // Update loading text
        loadingLayer.textContent = `Layer ${layerId}: ${layerInfo.name}`;

        // Update progress
        const progress = ((i + 1) / layers.length) * 100;
        progressBar.style.width = `${progress}%`;

        // Mark layer as running
        updateLayerStatus(layerId, 'running');

        // Wait a bit
        await new Promise(resolve => setTimeout(resolve, 100));
    }
}

/**
 * Run the DQS pipeline
 */
async function runPipeline() {
    const btn = document.getElementById('runPipelineBtn');
    const overlay = document.getElementById('loadingOverlay');
    const resultsSection = document.getElementById('resultsSection');

    // Disable button
    btn.disabled = true;
    btn.innerHTML = '<span class="btn-icon">⏳</span> Processing...';

    // Reset visuals
    resetLayerVisuals();

    // Show loading
    overlay.style.display = 'flex';
    document.getElementById('progressBar').style.width = '0%';

    // Start progress simulation
    const progressPromise = simulateProgress();

    try {
        // Get parameters
        const txnCount = document.getElementById('txnCount').value;
        const anomalyRate = document.getElementById('anomalyRate').value;
        const useAI = document.getElementById('useAI').checked;

        // Make API call
        const response = await fetch(`${API_BASE}/api/run`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                count: parseInt(txnCount),
                anomaly_rate: parseInt(anomalyRate) / 100,
                use_ai: useAI,
                seed: Date.now() % 10000
            })
        });

        // Wait for progress simulation to complete
        await progressPromise;

        const data = await response.json();

        if (data.success) {
            // Update layer visuals based on actual results
            updateLayerVisuals(data.layer_timings);

            // Update results
            updateResults(data);

            // Show results section
            resultsSection.style.display = 'block';
        } else {
            alert('Pipeline failed: ' + (data.error || 'Unknown error'));
            resetLayerVisuals();
        }
    } catch (error) {
        console.error('Pipeline error:', error);
        alert('Failed to run pipeline: ' + error.message);
        resetLayerVisuals();
    } finally {
        // Hide loading
        overlay.style.display = 'none';

        // Re-enable button
        btn.disabled = false;
        btn.innerHTML = '<span class="btn-icon">▶</span> Run Pipeline';
    }
}

/**
 * Update layer visuals based on timing results
 */
function updateLayerVisuals(timings) {
    timings.forEach(timing => {
        const status = timing.status.toUpperCase();
        let visualStatus = 'passed';

        if (status === 'FAILED') {
            visualStatus = 'failed';
        } else if (status === 'DEGRADED') {
            visualStatus = 'degraded';
        }

        updateLayerStatus(timing.layer_id, visualStatus);
    });
}

/**
 * Update all result displays
 */
function updateResults(data) {
    // Summary cards
    document.getElementById('safeCount').textContent = data.safe_count;
    document.getElementById('reviewCount').textContent = data.review_count;
    document.getElementById('escalateCount').textContent = data.escalate_count;
    document.getElementById('rejectedCount').textContent = data.rejected_count;

    // Metrics
    document.getElementById('qualityRate').textContent = data.quality_rate + '%';
    document.getElementById('avgDqs').textContent = data.average_dqs.toFixed(1);
    document.getElementById('totalRecords').textContent = data.total_records;
    document.getElementById('duration').textContent = data.total_duration_ms.toFixed(0);

    // Update circular progress
    updateCircularProgress('qualityFill', data.quality_rate);
    updateCircularProgress('dqsFill', data.average_dqs);

    // Layer table
    updateLayerTable(data.layer_timings, data.layer_details);

    // Reports
    document.getElementById('decisionReport').textContent = data.decision_report || 'No report available';
    document.getElementById('executionReport').textContent = data.execution_report || 'No report available';
}

/**
 * Update circular progress indicator
 */
function updateCircularProgress(elementId, percentage) {
    const circle = document.getElementById(elementId);
    if (circle) {
        const circumference = 283; // 2 * PI * 45
        const offset = circumference - (percentage / 100) * circumference;
        circle.style.strokeDashoffset = offset;
    }
}

/**
 * Update layer execution table
 */
function updateLayerTable(timings, details) {
    const tbody = document.getElementById('layerTableBody');
    tbody.innerHTML = '';

    timings.forEach(timing => {
        const detail = details[String(timing.layer_id)] || {};
        const row = document.createElement('tr');

        // Status badge class
        let statusClass = 'status-passed';
        if (timing.status.toUpperCase() === 'FAILED') {
            statusClass = 'status-failed';
        } else if (timing.status.toUpperCase() === 'DEGRADED') {
            statusClass = 'status-degraded';
        }

        // Get checks info
        const checksPerformed = detail.checks_performed || 0;
        const checksPassed = detail.checks_passed || 0;

        // Get detail summary
        let detailSummary = '';
        if (detail.details) {
            const d = detail.details;
            if (d.records_processed !== undefined) {
                detailSummary = `${d.records_processed} records`;
            } else if (d.features_extracted !== undefined) {
                detailSummary = `${d.features_extracted} features`;
            } else if (d.valid_records !== undefined) {
                detailSummary = `${d.valid_records} valid, ${d.rejected_records || 0} rejected`;
            } else if (d.dqs_mean !== undefined) {
                detailSummary = `Mean DQS: ${d.dqs_mean.toFixed(1)}`;
            } else if (d.records_flagged !== undefined) {
                detailSummary = `${d.records_flagged} flagged`;
            } else if (d.total_conflicts !== undefined) {
                detailSummary = `${d.total_conflicts} conflicts`;
            } else if (d.safe_count !== undefined) {
                detailSummary = `${d.safe_count} safe, ${d.review_count || 0} review`;
            } else {
                detailSummary = '-';
            }
        }

        row.innerHTML = `
            <td><strong>L${timing.layer_id}</strong></td>
            <td>${timing.layer_name}</td>
            <td><span class="status-badge ${statusClass}">${timing.status}</span></td>
            <td>${timing.duration_ms.toFixed(2)}ms</td>
            <td>${checksPassed}/${checksPerformed}</td>
            <td>${detailSummary}</td>
        `;

        tbody.appendChild(row);
    });
}

// Periodic health check
setInterval(checkHealth, 30000);
