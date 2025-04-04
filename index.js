require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cheerio = require('cheerio');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const { stringify, parse } = require('csv-stringify/sync');
const fs = require('fs');
const path = require('path');

const app = express();

// Middleware
app.use(cors());
app.use(express.json());

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
});
app.use(limiter);

// Ensure output directory exists
const outputDir = path.join(__dirname, 'output');
if (!fs.existsSync(outputDir)) {
  fs.mkdirSync(outputDir);
}

// Constant filename for the persistent CSV
const PERSISTENT_CSV_FILENAME = 'all_businesses.csv';

// Improved logging function
function log(context, message) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] [${context}] ${message}`);
}

async function scrapePages(baseUrl, startPage, endPage) {
  const context = 'ScrapePages';
  log(context, `Starting scrape from ${startPage} to ${endPage}`);

  const allBusinesses = [];
  const errors = [];
  const normalizedBaseUrl = baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`;

  // Load previously scraped businesses
  const previousBusinesses = loadPreviousBusinesses();
  const previousBusinessNames = new Set(previousBusinesses.map(b => b.name));
  log(context, `Loaded ${previousBusinesses.length} existing businesses`);

  for (let page = startPage; page <= endPage; page++) {
    const url = `${normalizedBaseUrl}${page}`;

    try {
      log(context, `Processing page ${page}: ${url}`);
      const response = await axios.get(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        },
        timeout: 10000
      });

      const $ = cheerio.load(response.data);
      let pageBusinessCount = 0;

      $('.company.g_0').each((index, element) => {
        try {
          // Skip ads
          if ($(element).hasClass('company_ad')) return;

          const name = $(element).find('.company_header h3 a').text().trim();
          const address = $(element).find('.company_header .address').text().trim();

          // Better duplicate detection
          const duplicateKey = `${name}|${address}`;
          if (previousBusinessNames.has(duplicateKey)) {
            log(context, `Skipping duplicate: ${name}`);
            return;
          }

          let phone = '';
          const phoneElement = $(element).find('.cont .s:has(.fa-phone) span');
          if (phoneElement.length) phone = phoneElement.text().trim();

          // Simplified image URL handling (since many don't have images)
          let imageUrl = '';

          const business = {
            name,
            address,
            phone,
            imageUrl, // Leave empty if no image
            verified: false, // The HTML doesn't show verification status
            category: categoryName,
            sourcePage: page,
            dateScraped: new Date().toISOString()
          };

          if (name && (address || phone)) {
            allBusinesses.push(business);
            previousBusinessNames.add(duplicateKey);
            pageBusinessCount++;
          }
        } catch (itemError) {
          log(context, `Error processing business on page ${page}: ${itemError.message}`);
        }
      });

      log(context, `Page ${page} completed. Found ${pageBusinessCount} new businesses`);
      await new Promise(resolve => setTimeout(resolve, 2000));

    } catch (error) {
      const errorMsg = `Error scraping page ${page}: ${error.message}`;
      log(context, errorMsg);
      errors.push({ page, url, error: error.message });
    }
  }

  // Combine new businesses with previous businesses
  const combinedBusinesses = [...previousBusinesses, ...allBusinesses];
  log(context, `Total businesses after scrape: ${combinedBusinesses.length}`);

  // Generate CSV
  const csvData = stringify(combinedBusinesses, {
    header: true,
    columns: [
      { key: 'name', header: 'Business Name' },
      { key: 'address', header: 'Address' },
      { key: 'phone', header: 'Phone Number' },
      { key: 'imageUrl', header: 'Image URL' },
      { key: 'verified', header: 'Verified' },
      { key: 'sourcePage', header: 'Page Number' },
      { key: 'dateScraped', header: 'Date Scraped' }
    ]
  });

  // Save to persistent CSV
  const filePath = path.join(outputDir, PERSISTENT_CSV_FILENAME);
  fs.writeFileSync(filePath, csvData);
  log(context, `CSV saved to: ${filePath}`);

  return {
    businesses: allBusinesses,
    totalBusinesses: combinedBusinesses.length,
    errors,
    stats: {
      totalPagesAttempted: endPage - startPage + 1,
      successfulPages: (endPage - startPage + 1) - errors.length,
      failedPages: errors.length,
      newBusinessesScraped: allBusinesses.length,
      totalBusinessesSaved: combinedBusinesses.length,
      csvFile: PERSISTENT_CSV_FILENAME
    }
  };
}

async function scrapeCategoryPages(baseUrl, startPage, endPage) {
  const context = 'ScrapeCategory';
  log(context, 'Request received');

  const allBusinesses = [];
  const errors = [];

  try {
    // Extract category name from URL
    const categoryMatch = baseUrl.match(/category\/([^\/]+)/i);
    const categoryName = categoryMatch ? categoryMatch[1] : 'unknown_category';
    const csvFilename = `${categoryName}.csv`;

    log(context, `Category: ${categoryName}, Output file: ${csvFilename}`);

    // Normalize base URL - handle both with and without city parameter
    let normalizedBaseUrl;
    if (baseUrl.includes('city:')) {
      // For URLs with city parameter like ".../category/others/1/city:lagos"
      normalizedBaseUrl = baseUrl.replace(/(\/\d+\/city:[^\/]+)$/, '');
    } else {
      // For standard category URLs
      normalizedBaseUrl = baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`;
    }

    log(context, `Normalized base URL: ${normalizedBaseUrl}`);

    // Load previous businesses for this category
    log(context, `Loading previous businesses from ${csvFilename}`);
    const previousBusinesses = loadPreviousBusinesses(csvFilename);
    const previousBusinessNames = new Set(previousBusinesses.map(b => `${b.name}|${b.address}`));
    log(context, `Found ${previousBusinesses.length} existing businesses`);

    for (let page = startPage; page <= endPage; page++) {
      // Construct URL with proper pagination
      let url;
      if (baseUrl.includes('city:')) {
        // Reconstruct URL with page number and city parameter
        url = `${normalizedBaseUrl}/${page}/city:${baseUrl.split('city:')[1]}`;
      } else {
        // Standard category URL
        url = `${normalizedBaseUrl}${page}`;
      }

      log(context, `Processing page ${page}: ${url}`);

      try {
        const response = await axios.get(url, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
          },
          timeout: 10000
        });

        const $ = cheerio.load(response.data);
        let pageBusinessCount = 0;

        // Handle both types of listings
        $('.company.g_0, .company.with_img.g_0').each((index, element) => {
          try {
            // Skip ads
            if ($(element).hasClass('company_ad')) return;

            // Extract business information - handle both formats
            const name = $(element).find('.company_header h3 a').text().trim();
            const address = $(element).find('.company_header .address').text().trim();

            // Better duplicate detection
            const duplicateKey = `${name}|${address}`;
            if (previousBusinessNames.has(duplicateKey)) {
              log(context, `Skipping duplicate: ${name}`);
              return;
            }

            let phone = '';
            const phoneElement = $(element).find('.cont .s:has(.fa-phone) span');
            if (phoneElement.length) phone = phoneElement.text().trim();

            // Handle image URL - different approaches for each format
            let imageUrl = '';
            if ($(element).hasClass('with_img')) {
              // For listings with images
              const logoElement = $(element).find('.logo.lazy-img');
              if (logoElement.length) {
                const relativePath = logoElement.attr('data-bg') || '';
                if (relativePath) {
                  // Construct full URL from relative path
                  if (relativePath.startsWith('http')) {
                    imageUrl = relativePath; // Already a full URL
                  } else if (relativePath.startsWith('/')) {
                    imageUrl = `https://www.businesslist.com.ng${relativePath}`;
                  } else {
                    imageUrl = `https://www.businesslist.com.ng/img/${relativePath}`;
                  }
                }
              }
            }

            // Check for verification status
            let verified = false;
            if ($(element).find('.cont u.v:has(.fa-check-circle)').length > 0) {
              verified = true;
            }

            const business = {
              name,
              address,
              phone,
              imageUrl,
              verified,
              category: categoryName,
              sourcePage: page,
              dateScraped: new Date().toISOString()
            };

            if (name && (address || phone)) {
              allBusinesses.push(business);
              previousBusinessNames.add(duplicateKey);
              pageBusinessCount++;
            }
          } catch (itemError) {
            log(context, `Error processing business on page ${page}: ${itemError.message}`);
          }
        });

        log(context, `Page ${page} completed. Found ${pageBusinessCount} new businesses`);
        await new Promise(resolve => setTimeout(resolve, 2000));

      } catch (error) {
        const errorMsg = `Error scraping page ${page}: ${error.message}`;
        log(context, errorMsg);
        errors.push({ page, url, error: error.message });

        // If we get multiple consecutive errors, break the loop
        if (errors.length > 5 && errors.slice(-5).every(e => e.error.includes('404'))) {
          log(context, 'Multiple consecutive 404 errors detected. Stopping scrape.');
          break;
        }
      }
    }

    // Combine new businesses with previous businesses
    const combinedBusinesses = [...previousBusinesses, ...allBusinesses];
    log(context, `Total businesses after scrape: ${combinedBusinesses.length}`);

    // Generate CSV
    const csvData = stringify(combinedBusinesses, {
      header: true,
      columns: [
        { key: 'name', header: 'Business Name' },
        { key: 'address', header: 'Address' },
        { key: 'phone', header: 'Phone Number' },
        { key: 'imageUrl', header: 'Image URL' },
        { key: 'verified', header: 'Verified' },
        { key: 'category', header: 'Category' },
        { key: 'sourcePage', header: 'Page Number' },
        { key: 'dateScraped', header: 'Date Scraped' }
      ]
    });

    // Save to category-specific CSV
    const filePath = path.join(outputDir, csvFilename);
    fs.writeFileSync(filePath, csvData);
    log(context, `CSV saved to: ${filePath}`);

    return {
      businesses: allBusinesses,
      totalBusinesses: combinedBusinesses.length,
      errors,
      stats: {
        totalPagesAttempted: endPage - startPage + 1,
        successfulPages: (endPage - startPage + 1) - errors.length,
        failedPages: errors.length,
        newBusinessesScraped: allBusinesses.length,
        totalBusinessesSaved: combinedBusinesses.length,
        csvFile: csvFilename,
        category: categoryName
      }
    };

  } catch (error) {
    log(context, `Critical error: ${error.message}`);
    throw error;
  }
}

function loadPreviousBusinesses(filename = PERSISTENT_CSV_FILENAME) {
  const context = 'LoadPreviousBusinesses';
  try {
    const filePath = path.join(outputDir, filename);

    if (!fs.existsSync(filePath)) {
      log(context, `No existing file found at ${filePath}`);
      return [];
    }

    const csvContent = fs.readFileSync(filePath, 'utf-8');
    const businesses = parse(csvContent, {
      columns: true,
      cast: (value, context) => {
        if (context.column === 'verified') {
          return value.toLowerCase() === 'true';
        }
        return value;
      }
    });

    log(context, `Successfully loaded ${businesses.length} businesses from ${filename}`);
    return businesses;
  } catch (error) {
    log(context, `Error loading businesses: ${error.message}`);
    return [];
  }
}

// API Endpoints
app.post('/scrape', async (req, res) => {
  const context = 'API /scrape';
  log(context, 'Request received');

  const { baseUrl, startPage, endPage } = req.body;

  // Validation
  if (!baseUrl || startPage === undefined || endPage === undefined) {
    const errorMsg = 'Missing parameters: Provide baseUrl, startPage, and endPage';
    log(context, errorMsg);
    return res.status(400).json({
      success: false,
      error: errorMsg
    });
  }

  if (startPage < 1 || endPage < 1 || startPage > endPage) {
    const errorMsg = 'Invalid page range: Page numbers must be positive and startPage ≤ endPage';
    log(context, errorMsg);
    return res.status(400).json({
      success: false,
      error: errorMsg
    });
  }

  try {
    log(context, 'Starting scrape...');
    const result = await scrapePages(baseUrl, startPage, endPage);
    log(context, `Scrape completed. Total businesses: ${result.totalBusinesses}`);

    res.json({
      success: true,
      ...result,
      downloadLink: `/download/${result.stats.csvFile}`
    });
  } catch (error) {
    log(context, `Scrape failed: ${error.message}`);
    res.status(500).json({
      success: false,
      error: error.message,
      details: 'Check server logs for more information'
    });
  }
});

app.post('/scrape-category', async (req, res) => {
  const context = 'API /scrape-category';
  log(context, 'Request received');
  log(context, `Request body: ${JSON.stringify(req.body)}`);

  const { baseUrl, startPage, endPage } = req.body;

  // Validation
  if (!baseUrl || startPage === undefined || endPage === undefined) {
    const errorMsg = 'Missing parameters: Provide baseUrl, startPage, and endPage';
    log(context, errorMsg);
    return res.status(400).json({
      success: false,
      error: errorMsg
    });
  }

  if (startPage < 1 || endPage < 1 || startPage > endPage) {
    const errorMsg = 'Invalid page range: Page numbers must be positive and startPage ≤ endPage';
    log(context, errorMsg);
    return res.status(400).json({
      success: false,
      error: errorMsg
    });
  }

  if (!baseUrl.includes('/category/')) {
    const errorMsg = 'Invalid URL: URL must be a category URL (contain /category/)';
    log(context, errorMsg);
    return res.status(400).json({
      success: false,
      error: errorMsg
    });
  }

  try {
    log(context, 'Starting category scrape...');
    const result = await scrapeCategoryPages(baseUrl, startPage, endPage);
    log(context, `Scrape completed. Stats: ${JSON.stringify(result.stats)}`);

    res.json({
      success: true,
      ...result,
      downloadLink: `/download/${result.stats.csvFile}`
    });
  } catch (error) {
    log(context, `Scrape failed: ${error.message}`);
    res.status(500).json({
      success: false,
      error: error.message,
      details: 'Check server logs for more information'
    });
  }
});

// Download endpoint
app.get('/download/:filename', (req, res) => {
  const context = 'API /download';
  const filePath = path.join(outputDir, req.params.filename);

  if (fs.existsSync(filePath)) {
    log(context, `Serving file: ${filePath}`);
    res.download(filePath);
  } else {
    const errorMsg = `File not found: ${req.params.filename}`;
    log(context, errorMsg);
    res.status(404).json({ error: errorMsg });
  }
});

app.get('/', (req, res) => {
  res.json({
    status: 'running',
    endpoints: {
      scrape: {
        method: 'POST',
        path: '/scrape',
        parameters: {
          baseUrl: 'URL without page number (e.g., "https://www.businesslist.com.ng/location/lagos/")',
          startPage: 'Integer ≥ 1',
          endPage: 'Integer ≥ startPage'
        }
      },
      'scrape-category': {
        method: 'POST',
        path: '/scrape-category',
        parameters: {
          baseUrl: 'Category URL (e.g., "https://www.businesslist.com.ng/category/others/1/city:lagos")',
          startPage: 'Integer ≥ 1',
          endPage: 'Integer ≥ startPage'
        },
        description: 'Scrape businesses by category, output named after category'
      },
      download: {
        method: 'GET',
        path: '/download/:filename',
        description: 'Download CSV file'
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`CSV files will be saved in: ${outputDir}`);
});