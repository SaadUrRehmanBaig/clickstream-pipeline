(function() {
    const track = (eventType, extraData = {}) => {
        const payload = {
            event_id: crypto.randomUUID(),
            event_type: eventType,
            url: window.location.href,
            referrer: document.referrer,
            timestamp: new Date().toISOString(),
            screen_width: window.innerWidth,
            screen_height: window.innerHeight,
            ...extraData
        };

        // Use sendBeacon to ensure delivery even if the user navigates away instantly
        navigator.sendBeacon('/api/collect', JSON.stringify(payload));
    };

    // Track initial page view
    track('page_view');

    document.addEventListener('click', (e) => {
    track('click', {
        element_tag: e.target.tagName,
        element_id: e.target.id || null,
        element_classes: e.target.className || null,
        element_text: e.target.innerText?.substring(0, 50) || null,
    });
});

})();