async function logMessage(message, level = 'info') {
    // Validate the log level
    const validLevels = ['debug', 'info', 'warning', 'error', 'critical'];
    if (!validLevels.includes(level)) {
        throw new Error(`Invalid log level. Must be one of: ${validLevels.join(', ')}`);
    }

    try {
        const response = await fetch('/api/logging', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                message: message,
                level: level
            })
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        return await response.json();
    } catch (error) {
        console.error('Error logging message:', error);
        throw error;
    }
}