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

    // Track global click interactions
    document.addEventListener('click', (e) => {
        const target = e.target.closest('a, button, [data-track]');
        if (!target) return;

        track('click', {
            element_tag: target.tagName,
            element_id: target.id || null,
            element_classes: target.className || null,
            element_text: target.innerText?.substring(0, 50) || null,
            target_href: target.href || null
        });
    });
})();