/**
 * DQS Engine Dashboard - Enhanced JavaScript
 * Handles file uploads, API communication, animations, and UI updates
 */

const API_BASE = '';

// Layer info for animations
const LAYER_INFO = {
    1: { name: 'Input Contract', phase: 1, phaseName: 'Foundation' },
    2: { name: 'Input Validation', phase: 1, phaseName: 'Foundation' },
    3: { name: 'Feature Extraction', phase: 2, phaseName: 'Feature Extraction' },
    4.1: { name: 'Structural Integrity', phase: 3, phaseName: 'Deterministic Inference' },
    4.2: { name: 'Field Compliance', phase: 3, phaseName: 'Deterministic Inference' },
    4.3: { name: 'Semantic Validation', phase: 3, phaseName: 'Deterministic Inference' },
    4.4: { name: 'Anomaly Detection', phase: 4, phaseName: 'AI Inference' },
    4.5: { name: 'GenAI Summarization', phase: 4, phaseName: 'AI Inference' },
    5: { name: 'Output Contract', phase: 5, phaseName: 'Output & Decision' },
    6: { name: 'Stability & Consistency', phase: 5, phaseName: 'Output & Decision' },
    7: { name: 'Conflict Detection', phase: 5, phaseName: 'Output & Decision' },
    8: { name: 'Confidence Band', phase: 5, phaseName: 'Output & Decision' },
    9: { name: 'Decision Gate', phase: 6, phaseName: 'Decision Gate' },
    10: { name: 'Responsibility Boundary', phase: 6, phaseName: 'Decision Gate' },
    11: { name: 'Logging & Trace', phase: 7, phaseName: 'Logging' }
};

const LAYER_ORDER = [1, 2, 3, 4.1, 4.2, 4.3, 4.4, 4.5, 5, 6, 7, 8, 9, 10, 11];

// State
let currentTab = 'generate';
let customData = null;
let uploadedFileName = '';
let errorTrace = '';

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    checkHealth();
    resetLayerVisuals();
    setupDragDrop();
});

// Tab switching
function switchTab(tabName) {
    currentTab = tabName;

    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tab === tabName);
    });

    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.toggle('active', content.id === `tab-${tabName}`);
    });
}

// Slider updates
function updateSlider(sliderId, valueId) {
    const slider = document.getElementById(sliderId);
    const valueSpan = document.getElementById(valueId);

    if (sliderId === 'anomalyRate') {
        valueSpan.textContent = slider.value + '%';
    } else {
        valueSpan.textContent = slider.value;
    }
}

// Health check
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

// JSON validation
function validateJson() {
    const textarea = document.getElementById('jsonInput');
    const statusEl = document.getElementById('jsonStatus');

    try {
        const data = JSON.parse(textarea.value);
        customData = Array.isArray(data) ? data : [data];
        statusEl.textContent = `Valid JSON - ${customData.length} record(s)`;
        statusEl.className = 'json-status valid';
        showDataPreview(customData);
    } catch (e) {
        statusEl.textContent = 'Invalid JSON: ' + e.message;
        statusEl.className = 'json-status invalid';
        customData = null;
    }
}

// Load sample JSON
async function loadSampleJson() {
    try {
        const response = await fetch(`${API_BASE}/api/generate`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ count: 3 })
        });
        const data = await response.json();

        if (data.success && data.preview) {
            document.getElementById('jsonInput').value = JSON.stringify(data.preview, null, 2);
            validateJson();
        }
    } catch (e) {
        showError('Failed to load sample', e.message);
    }
}

// File upload handling
function setupDragDrop() {
    const dropZone = document.getElementById('dropZone');

    dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropZone.classList.add('dragover');
    });

    dropZone.addEventListener('dragleave', () => {
        dropZone.classList.remove('dragover');
    });

    dropZone.addEventListener('drop', (e) => {
        e.preventDefault();
        dropZone.classList.remove('dragover');

        const files = e.dataTransfer.files;
        if (files.length > 0) {
            handleFile(files[0]);
        }
    });
}

function handleFileUpload(event) {
    const file = event.target.files[0];
    if (file) {
        handleFile(file);
    }
}

function handleFile(file) {
    if (!file.name.endsWith('.csv')) {
        showError('Invalid file type', 'Please upload a CSV file');
        return;
    }

    uploadedFileName = file.name;
    document.getElementById('fileName').textContent = file.name;
    document.getElementById('fileSize').textContent = formatFileSize(file.size);
    document.getElementById('fileInfo').style.display = 'flex';
    document.getElementById('dropZone').style.display = 'none';

    // Read and parse CSV
    const reader = new FileReader();
    reader.onload = (e) => {
        try {
            customData = parseCSV(e.target.result);
            showDataPreview(customData);
        } catch (err) {
            showError('CSV Parse Error', err.message);
        }
    };
    reader.readAsText(file);
}

function parseCSV(csvText) {
    const lines = csvText.trim().split('\n');
    const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));

    const data = [];
    for (let i = 1; i < lines.length; i++) {
        const values = lines[i].split(',').map(v => v.trim().replace(/"/g, ''));
        const row = {};
        headers.forEach((h, idx) => {
            row[h] = values[idx] || '';
        });
        data.push(row);
    }

    return data;
}

function clearFile() {
    customData = null;
    uploadedFileName = '';
    document.getElementById('fileInfo').style.display = 'none';
    document.getElementById('dropZone').style.display = 'block';
    document.getElementById('csvFile').value = '';
    document.getElementById('dataPreviewSection').style.display = 'none';
}

function formatFileSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

// Data preview
function showDataPreview(data) {
    if (!data || data.length === 0) return;

    const section = document.getElementById('dataPreviewSection');
    const countEl = document.getElementById('previewCount');
    const thead = document.getElementById('previewTableHead');
    const tbody = document.getElementById('previewTableBody');

    section.style.display = 'block';
    countEl.textContent = `${data.length} record(s)`;

    // Get keys from first record
    const keys = Object.keys(data[0]).slice(0, 8); // Limit columns

    // Header
    thead.innerHTML = `<tr>${keys.map(k => `<th>${formatKey(k)}</th>`).join('')}</tr>`;

    // Body (first 10 rows)
    tbody.innerHTML = data.slice(0, 10).map(row => {
        return `<tr>${keys.map(k => `<td>${formatValue(row[k])}</td>`).join('')}</tr>`;
    }).join('');
}

function formatKey(key) {
    return key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
}

function formatValue(val) {
    if (val === null || val === undefined) return '-';
    if (typeof val === 'object') return JSON.stringify(val).slice(0, 30) + '...';
    const str = String(val);
    return str.length > 30 ? str.slice(0, 30) + '...' : str;
}

// Reset layer visuals
function resetLayerVisuals() {
    document.querySelectorAll('.layer').forEach(layer => {
        layer.classList.remove('passed', 'failed', 'degraded', 'running');
    });
    document.querySelectorAll('.phase').forEach(phase => {
        phase.classList.remove('active', 'completed');
    });
}

// Update single layer status
function updateLayerStatus(layerId, status) {
    const layer = document.querySelector(`[data-layer="${layerId}"]`);
    if (layer) {
        layer.classList.remove('passed', 'failed', 'degraded', 'running');
        if (status) {
            layer.classList.add(status.toLowerCase());
        }
    }
}

// Update phase status
function updatePhaseStatus(phaseNum, status) {
    const phase = document.querySelector(`[data-phase="${phaseNum}"]`);
    if (phase) {
        phase.classList.remove('active', 'completed');
        if (status) {
            phase.classList.add(status);
        }
    }
}

// Animate pipeline progress
async function animatePipeline() {
    const progressBar = document.getElementById('progressBar');
    const loadingLayer = document.getElementById('loadingLayer');
    const loadingPhase = document.getElementById('loadingPhase');
    const loadingPercent = document.getElementById('loadingPercent');

    let currentPhase = 0;

    for (let i = 0; i < LAYER_ORDER.length; i++) {
        const layerId = LAYER_ORDER[i];
        const info = LAYER_INFO[layerId];

        // Update loading text
        loadingLayer.textContent = `Layer ${layerId}: ${info.name}`;
        loadingPhase.textContent = `Phase ${info.phase}: ${info.phaseName}`;

        // Update progress
        const progress = ((i + 1) / LAYER_ORDER.length) * 100;
        progressBar.style.width = `${progress}%`;
        loadingPercent.textContent = `${Math.round(progress)}%`;

        // Update phase animation
        if (info.phase !== currentPhase) {
            if (currentPhase > 0) {
                updatePhaseStatus(currentPhase, 'completed');
            }
            currentPhase = info.phase;
            updatePhaseStatus(currentPhase, 'active');
        }

        // Mark layer as running
        updateLayerStatus(layerId, 'running');

        await new Promise(resolve => setTimeout(resolve, 80));

        // Mark layer as passed (temporarily, will be updated with real status)
        updateLayerStatus(layerId, 'passed');
    }

    // Complete last phase
    updatePhaseStatus(currentPhase, 'completed');
}

// Run pipeline
async function runPipeline() {
    const btn = document.getElementById('runPipelineBtn');
    const overlay = document.getElementById('loadingOverlay');
    const resultsSection = document.getElementById('resultsSection');

    btn.disabled = true;
    btn.innerHTML = '<span class="btn-icon">⏳</span> Processing...';

    resetLayerVisuals();
    overlay.style.display = 'flex';
    document.getElementById('progressBar').style.width = '0%';

    // Start animation
    const animPromise = animatePipeline();

    try {
        let requestBody;

        if (currentTab === 'generate') {
            requestBody = {
                count: parseInt(document.getElementById('txnCount').value),
                anomaly_rate: parseInt(document.getElementById('anomalyRate').value) / 100,
                use_ai: document.getElementById('useAI').checked,
                seed: Date.now() % 10000
            };
        } else {
            requestBody = {
                custom_data: customData,
                use_ai: document.getElementById('useAI').checked
            };
        }

        const response = await fetch(`${API_BASE}/api/run`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(requestBody)
        });

        await animPromise;

        const data = await response.json();

        if (data.success) {
            updateLayerVisualsFromResults(data.layer_timings);
            updateResults(data);
            resultsSection.style.display = 'block';
            resultsSection.scrollIntoView({ behavior: 'smooth' });
        } else {
            errorTrace = data.traceback || '';
            showError('Pipeline failed', data.error || 'Unknown error occurred');
            resetLayerVisuals();
        }
    } catch (error) {
        console.error('Pipeline error:', error);
        errorTrace = error.stack || '';
        showError('Connection failed', error.message);
        resetLayerVisuals();
    } finally {
        overlay.style.display = 'none';
        btn.disabled = false;
        btn.innerHTML = '<span class="btn-icon">▶</span> Run Quality Analysis';
    }
}

// Update layer visuals from results
function updateLayerVisualsFromResults(timings) {
    timings.forEach(t => {
        const status = t.status.toUpperCase();
        let visualStatus = 'passed';
        if (status === 'FAILED') visualStatus = 'failed';
        else if (status === 'DEGRADED') visualStatus = 'degraded';
        updateLayerStatus(t.layer_id, visualStatus);
    });
}

// Update all results
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

    // Circular progress
    updateCircularProgress('qualityFill', data.quality_rate);
    updateCircularProgress('dqsFill', data.average_dqs);

    // Layer table
    updateLayerTable(data.layer_timings, data.layer_details);

    // AI Analysis
    generateAnalysisContent(data);

    // Review records
    generateReviewCards(data);

    // Reports (formatted)
    document.getElementById('decisionReport').innerHTML = formatReport(data.decision_report);
    document.getElementById('executionReport').innerHTML = formatReport(data.execution_report);
}

// Update circular progress
function updateCircularProgress(elementId, percentage) {
    const circle = document.getElementById(elementId);
    if (circle) {
        const circumference = 283;
        const offset = circumference - (percentage / 100) * circumference;
        circle.style.strokeDashoffset = offset;
    }
}

// Update layer table
function updateLayerTable(timings, details) {
    const tbody = document.getElementById('layerTableBody');
    tbody.innerHTML = '';

    timings.forEach(t => {
        const detail = details[String(t.layer_id)] || {};
        const row = document.createElement('tr');

        const statusClass = `status-${t.status.toUpperCase()}`;
        const checksPerformed = detail.checks_performed || 0;
        const checksPassed = detail.checks_passed || 0;

        let detailText = getLayerDetailText(t.layer_id, detail.details);

        row.innerHTML = `
            <td><strong>L${t.layer_id}</strong></td>
            <td>${t.layer_name}</td>
            <td><span class="status-badge ${statusClass}">${t.status}</span></td>
            <td>${t.duration_ms.toFixed(2)}ms</td>
            <td>${checksPassed}/${checksPerformed}</td>
            <td>${detailText}</td>
        `;

        tbody.appendChild(row);
    });
}

// Get readable layer detail text
function getLayerDetailText(layerId, details) {
    if (!details) return '-';

    const d = details;

    if (d.records_processed !== undefined) return `${d.records_processed} records processed`;
    if (d.features_extracted !== undefined) return `${d.features_extracted} features extracted`;
    if (d.valid_records !== undefined) return `${d.valid_records} valid, ${d.rejected_records || 0} rejected`;
    if (d.dqs_mean !== undefined) return `Mean DQS: ${Number(d.dqs_mean).toFixed(1)}`;
    if (d.records_flagged !== undefined) return `${d.records_flagged} records flagged`;
    if (d.total_conflicts !== undefined) return `${d.total_conflicts} conflicts detected`;
    if (d.safe_count !== undefined) return `${d.safe_count} safe, ${d.review_count || 0} review`;
    if (d.pending_reviews !== undefined) return `${d.pending_reviews} pending reviews`;
    if (d.records_logged !== undefined) return `${d.records_logged} records logged`;

    return '-';
}

// Generate AI analysis content
function generateAnalysisContent(data) {
    const container = document.getElementById('analysisContent');

    let html = '';

    // Quality summary
    const qualityClass = data.quality_rate >= 80 ? 'success' : data.quality_rate >= 50 ? 'warning' : 'danger';
    html += `
        <div class="analysis-item ${qualityClass}">
            <div class="analysis-title">📊 Overall Quality Assessment</div>
            <div class="analysis-text">
                The pipeline processed ${data.total_records} transactions with a quality rate of ${data.quality_rate}%. 
                ${data.safe_count} records passed all validation checks and are ready for processing. 
                The average Data Quality Score is ${data.average_dqs.toFixed(1)} out of 100.
            </div>
        </div>
    `;

    // Review needed
    if (data.review_count > 0 || data.escalate_count > 0) {
        html += `
            <div class="analysis-item warning">
                <div class="analysis-title">⚠ Records Requiring Attention</div>
                <div class="analysis-text">
                    ${data.review_count} record(s) require human review due to detected anomalies or low confidence scores.
                    ${data.escalate_count > 0 ? `Additionally, ${data.escalate_count} record(s) need urgent escalation.` : ''}
                    Please examine the flagged transactions in the Review Section below.
                </div>
            </div>
        `;
    }

    // Rejected
    if (data.rejected_count > 0) {
        html += `
            <div class="analysis-item danger">
                <div class="analysis-title">❌ Rejected Records</div>
                <div class="analysis-text">
                    ${data.rejected_count} record(s) failed structural or semantic validation and cannot be processed.
                    These records contain critical data quality issues such as missing required fields, 
                    invalid data types, or business rule violations.
                </div>
            </div>
        `;
    }

    // Performance
    html += `
        <div class="analysis-item">
            <div class="analysis-title">⚡ Pipeline Performance</div>
            <div class="analysis-text">
                All 15 layers executed successfully in ${data.total_duration_ms.toFixed(0)}ms.
                The pipeline processed an average of ${(data.total_records / (data.total_duration_ms / 1000)).toFixed(0)} records per second.
            </div>
        </div>
    `;

    container.innerHTML = html;
}

// Generate review cards
function generateReviewCards(data) {
    const section = document.getElementById('reviewRecordsSection');
    const container = document.getElementById('reviewCards');
    const badge = document.getElementById('reviewBadge');

    const needsReview = data.review_count + data.escalate_count;

    if (needsReview === 0) {
        section.style.display = 'none';
        return;
    }

    section.style.display = 'block';
    badge.textContent = needsReview;

    // Parse decision report to extract flagged records
    const report = data.decision_report || '';
    const lines = report.split('\n');
    let html = '';

    let inReviewSection = false;
    let currentRecord = null;

    for (const line of lines) {
        if (line.includes('RECORDS REQUIRING REVIEW') || line.includes('ESCALATE')) {
            inReviewSection = true;
            continue;
        }

        if (inReviewSection && line.includes('txn_')) {
            const match = line.match(/txn_\d+/);
            if (match) {
                const txnId = match[0];
                const isEscalate = line.toLowerCase().includes('escalate');
                const reason = extractReason(line);

                html += `
                    <div class="review-card">
                        <div class="review-card-header">
                            <span class="review-card-id">${txnId}</span>
                            <span class="review-card-action ${isEscalate ? 'action-escalate' : 'action-review'}">
                                ${isEscalate ? 'Escalate' : 'Review'}
                            </span>
                        </div>
                        <div class="review-card-reason">${reason}</div>
                        <div class="review-card-meta">
                            <span class="meta-item">🔍 Requires manual verification</span>
                        </div>
                    </div>
                `;
            }
        }
    }

    if (!html) {
        html = `
            <div class="review-card">
                <div class="review-card-header">
                    <span class="review-card-id">Multiple Records</span>
                    <span class="review-card-action action-review">Review</span>
                </div>
                <div class="review-card-reason">
                    ${needsReview} record(s) flagged for review. 
                    Please check the Decision Report for detailed information.
                </div>
            </div>
        `;
    }

    container.innerHTML = html;
}

function extractReason(line) {
    // Extract meaningful reason from the line
    if (line.includes('Anomaly')) return 'Detected unusual patterns in transaction data';
    if (line.includes('Low DQS')) return 'Data Quality Score below acceptable threshold';
    if (line.includes('Conflict')) return 'Conflicting signals between validation layers';
    if (line.includes('Low confidence')) return 'Low confidence in quality assessment';
    return 'Requires human verification for data quality concerns';
}

// Format report for display (clean up symbols)
function formatReport(report) {
    if (!report) return '<span style="color: var(--text-muted)">No report available</span>';

    return report
        .replace(/\*\*/g, '')  // Remove **
        .replace(/\*/g, '')    // Remove *
        .replace(/#{1,6}\s/g, '')  // Remove markdown headers
        .replace(/`/g, '')     // Remove backticks
        .replace(/\[(\w+)\]/g, '$1')  // Clean up [OK] etc
        .replace(/={3,}/g, '─'.repeat(50))  // Replace === with line
        .replace(/-{3,}/g, '─'.repeat(30))  // Replace --- with line
        .split('\n')
        .map(line => {
            // Highlight key terms
            line = line.replace(/(SAFE_TO_USE|SAFE)/g, '<span style="color: #10b981">$1</span>');
            line = line.replace(/(REVIEW_REQUIRED|REVIEW)/g, '<span style="color: #f59e0b">$1</span>');
            line = line.replace(/(ESCALATE)/g, '<span style="color: #ef4444">$1</span>');
            line = line.replace(/(NO_ACTION|REJECTED)/g, '<span style="color: #6b7280">$1</span>');
            line = line.replace(/(PASSED)/gi, '<span style="color: #10b981">$1</span>');
            line = line.replace(/(FAILED)/gi, '<span style="color: #ef4444">$1</span>');
            line = line.replace(/(DEGRADED)/gi, '<span style="color: #f59e0b">$1</span>');
            return line;
        })
        .join('\n');
}

// Copy report to clipboard
function copyReport(elementId) {
    const content = document.getElementById(elementId).innerText;
    navigator.clipboard.writeText(content).then(() => {
        const btn = event.target;
        const originalText = btn.textContent;
        btn.textContent = 'Copied!';
        setTimeout(() => btn.textContent = originalText, 2000);
    });
}

// Error handling
function showError(title, message) {
    document.getElementById('errorMessage').textContent = message;
    document.getElementById('errorTrace').textContent = errorTrace || 'No additional details available';
    document.getElementById('errorModal').style.display = 'flex';
}

function closeErrorModal() {
    document.getElementById('errorModal').style.display = 'none';
    document.getElementById('errorDetails').style.display = 'none';
    errorTrace = '';
}

function toggleErrorDetails() {
    const details = document.getElementById('errorDetails');
    details.style.display = details.style.display === 'none' ? 'block' : 'none';
}

// Periodic health check
setInterval(checkHealth, 30000);
