const { Readability } = require('@mozilla/readability');
const { JSDOM } = require('jsdom');
const createDOMPurify = require('dompurify');

// Function to process the article
function ProcessArticle(html, url) {
    try {
        // Create a JSDOM instance with the HTML content and the correct URL
        const dom = new JSDOM(html, { url: url });

        // Initialize DOMPurify using the JSDOM window
        const window = dom.window;
        const DOMPurify = createDOMPurify(window);

        // Use Readability to parse the document
        const reader = new Readability(dom.window.document, { disableJSONLD: false });
        let article = reader.parse();

        // Sanitize the content using DOMPurify for security
        article.content = DOMPurify.sanitize(article.content);

        return article; // Return the parsed article object
    } catch (error) {
        // Log the error for debugging purposes
        console.error(`Error in ProcessArticle for URL: ${url} - ${error.message}`);
        return null; // Return null to indicate failure
    }
}

// Export the function as a CommonJS module
module.exports = { ProcessArticle };
