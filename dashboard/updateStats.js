// Use service names + container port (internal port, not host-mapped port)
const PROCESSING_STATS_API_URL = "http://localhost:8082/statistics";

const ANALYZER_API_URL = {
    snow: "http://localhost:8083/arrival/latest",
    lift: "http://localhost:8083/delay/latest",
    stats: "http://localhost:8083/stats/"
};

// NEW: Centralized Health Check Service
const HEALTH_API_URL = "http://localhost:8120/health-status";

// This function fetches and updates any endpoint
const makeReq = (url, cb) => {
    fetch(url)
        .then(res => res.json())
        .then((result) => {
            console.log("Received data: ", result);
            cb(result);
        })
        .catch((error) => {
            updateErrorMessages(error.message);
        });
};

// Update a code div
const updateCodeDiv = (result, elemId) =>
    document.getElementById(elemId).innerText = JSON.stringify(result);

// Update a status span (health check)
const updateStatusSpan = (statusObj, key, elemId) => {
    const val = statusObj[key] || "Down";
    const span = document.getElementById(elemId);

    if (span) {
        span.innerText = val;
        span.style.color = val === "Up" ? "green" : "red";
    }
};

const getLocaleDateStr = () => (new Date()).toLocaleString();

// Main stats fetch
const getStats = () => {
    document.getElementById("last-updated-value").innerText = getLocaleDateStr();

    // Processing + Analyzer Stats
    makeReq(PROCESSING_STATS_API_URL, (result) =>
        updateCodeDiv(result, "processing-stats")
    );

    makeReq(ANALYZER_API_URL.stats, (result) =>
        updateCodeDiv(result, "analyzer-stats")
    );

    makeReq(ANALYZER_API_URL.snow, (result) =>
        updateCodeDiv(result, "event-snow")
    );

    makeReq(ANALYZER_API_URL.lift, (result) =>
        updateCodeDiv(result, "event-lift")
    );

    // ✅ FIXED: Centralized Health Check Service
    makeReq(HEALTH_API_URL, (result) => {
        updateStatusSpan(result, "receiver", "receiver-status");
        updateStatusSpan(result, "storage", "storage-status");
        updateStatusSpan(result, "processing", "processing-status");
        updateStatusSpan(result, "analyzer", "analyzer-status");
    });
};

// Show errors in messages div
const updateErrorMessages = (message) => {
    const id = Date.now();

    const msg = document.createElement("div");
    msg.id = `error-${id}`;
    msg.innerHTML = `<p>Something happened at ${getLocaleDateStr()}!</p><code>${message}</code>`;

    document.getElementById("messages").style.display = "block";
    document.getElementById("messages").prepend(msg);

    setTimeout(() => {
        const elem = document.getElementById(`error-${id}`);
        if (elem) elem.remove();
    }, 7000);
};

// Setup periodic updates
const setup = () => {
    getStats();
    setInterval(getStats, 4000); // Update every 4 seconds
};

document.addEventListener("DOMContentLoaded", setup);