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

        return JSON.stringify(article); // Return the parsed article object as a JSON string
    } catch (error) {
        // Log the error for debugging purposes
        console.error(`Error in ProcessArticle for URL: ${url} - ${error.message}`);
        process.exit(1); // Indicate an error occurred
    }
}

// Read the URL from the command-line arguments
const url = process.argv[2];

// Read the HTML content from stdin
let html = '';
process.stdin.setEncoding('utf8');

process.stdin.on('data', function(chunk) {
    html += chunk;
});

process.stdin.on('end', function() {
    // Process the article after all data has been received
    const result = ProcessArticle(html, url);
    if (result) {
        console.log(result);
    }
});
