/**
 * ProcessArticlePersistent.js
 */

const { Readability } = require('@mozilla/readability');
const { JSDOM, VirtualConsole } = require('jsdom');
const createDOMPurify = require('dompurify');
const readline = require('readline');

/**
 * Processes HTML content to extract and sanitize the main article.
 *
 * @param {string} html - The HTML content of the webpage.
 * @param {string} url  - The URL of the webpage.
 * @returns {object}    - The extracted article object, or an error object.
 */
function processArticle(html, url) {
    try {
        // Create a virtual console to suppress JSDOM warnings (e.g. CSS errors).
        const virtualConsole = new VirtualConsole();
        // If you want to see warnings, you can uncomment:
        // virtualConsole.on("error", console.error);

        // Initialize JSDOM with the virtual console
        const dom = new JSDOM(html, { url, virtualConsole });

        // Prepare DOMPurify
        const DOMPurify = createDOMPurify(dom.window);

        // Use Mozilla's Readability
        const reader = new Readability(dom.window.document);

        // We have removed the "isProbablyReaderable()" check.
        // If you'd like to gate pages by “readability,” you can restore it,
        // but confirm whether your Readability version offers it or not.

        // Attempt to parse the article
        const article = reader.parse();
        if (!article) {
            throw new Error('Readability failed to parse the article.');
        }

        // Sanitize the Readability-parsed HTML content
        article.content = DOMPurify.sanitize(article.content);

        // Return the final article object
        return article;
    } catch (error) {
        console.error(`Error processing article from URL: ${url} - ${error.message}`);
        return { error: `Processing failed: ${error.message}` };
    }
}

// Create an interface to read JSON requests from stdin, line by line
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: false,
});

// Listen for incoming lines (each line should be a JSON string)
rl.on('line', (line) => {
    try {
        // Parse the incoming request
        const { html, url } = JSON.parse(line);
        if (!html || !url) {
            throw new Error('Invalid input: Missing HTML content or URL.');
        }

        // Process the article and output the JSON result
        const article = processArticle(html, url);
        console.log(JSON.stringify(article));
    } catch (e) {
        // Log error to stderr
        console.error(`Failed to process line: ${e.message}`);
        // Respond with a JSON error object
        console.log(JSON.stringify({ error: e.message }));
    }
});

/**
 * Catch any uncaught exceptions to prevent the process from crashing.
 * Depending on your preferences, you might choose to exit the process.
 */
process.on('uncaughtException', (err) => {
    console.error(`Uncaught exception: ${err.message}`);
});

/**
 * Catch unhandled promise rejections.
 */
process.on('unhandledRejection', (reason) => {
    console.error(`Unhandled rejection: ${reason}`);
});
