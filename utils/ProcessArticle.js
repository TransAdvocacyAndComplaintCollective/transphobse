const { Readability } = require('@mozilla/readability');
const { JSDOM } = require('jsdom');
const createDOMPurify = require('dompurify');

/**
 * Processes HTML content to extract and sanitize the main article.
 *
 * @param {string} html - The HTML content of the webpage.
 * @param {string} url - The URL of the webpage.
 * @returns {string} - The extracted article as a JSON string.
 */
function processArticle(html, url) {
    try {
        // Initialize JSDOM with the provided HTML and URL
        const dom = new JSDOM(html, { url });

        // Initialize DOMPurify with the JSDOM window
        const DOMPurify = createDOMPurify(dom.window);

        // Use Readability to parse the document
        const reader = new Readability(dom.window.document);
        const article = reader.parse();

        if (!article) {
            throw new Error('Readability failed to parse the article.');
        }

        // Sanitize the extracted content
        article.content = DOMPurify.sanitize(article.content);

        return JSON.stringify(article);
    } catch (error) {
        console.error(`Error processing article from URL: ${url} - ${error.message}`);
        process.exit(1);
    }
}

// Entry point
(async () => {
    try {
        const url = process.argv[2];
        if (!url) {
            throw new Error('No URL provided. Usage: node ProcessArticle.js <URL>');
        }

        let html = '';
        process.stdin.setEncoding('utf8');

        for await (const chunk of process.stdin) {
            html += chunk;
        }

        const result = processArticle(html, url);
        if (result) {
            console.log(result);
        }
    } catch (error) {
        console.error(`Unexpected error: ${error.message}`);
        process.exit(1);
    }
})();
