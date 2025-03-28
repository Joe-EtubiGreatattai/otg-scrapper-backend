require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cheerio = require('cheerio');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const { stringify } = require('csv-stringify/sync');
const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');

// Configure puppeteer with stealth plugin
puppeteer.use(StealthPlugin());

const app = express();

// Middleware
app.use(cors());
app.use(express.json());

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 30, // Very conservative limit
});
app.use(limiter);

// Enhanced headers configuration with rotation
const getRequestHeaders = (pageNumber) => {
  const userAgents = [
    // Chrome
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    // Firefox
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/115.0',
    // Safari
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15',
    // Edge
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0'
  ];

  const referers = [
    'https://www.google.com/',
    'https://www.bing.com/',
    'https://search.yahoo.com/',
    'https://duckduckgo.com/'
  ];

  return {
    'User-Agent': userAgents[pageNumber % userAgents.length],
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': referers[pageNumber % referers.length],
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'cross-site',
    'Sec-Fetch-User': '?1'
  };
};

// Output directory setup
const outputDir = path.join(__dirname, 'output');
if (!fs.existsSync(outputDir)) {
  fs.mkdirSync(outputDir, { recursive: true });
}

// Browser-based scraping fallback
async function scrapeWithBrowser(url) {
  let browser;
  try {
    browser = await puppeteer.launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-accelerated-2d-canvas',
        '--disable-gpu',
        `--proxy-server=${process.env.PROXY_URL || ''}`
      ],
      executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined
    });

    const page = await browser.newPage();
    
    // Set realistic viewport
    await page.setViewport({
      width: 1366,
      height: 768,
      deviceScaleFactor: 1,
      hasTouch: false,
      isLandscape: false,
      isMobile: false
    });

    // Set headers
    await page.setExtraHTTPHeaders(getRequestHeaders(Math.floor(Math.random() * 100)));

    // Random delays to simulate human behavior
    await page.waitForTimeout(Math.random() * 3000 + 2000);
    
    console.log(`Loading page with browser: ${url}`);
    await page.goto(url, {
      waitUntil: 'networkidle2',
      timeout: 30000
    });

    // Additional random delay after page load
    await page.waitForTimeout(Math.random() * 5000 + 2000);

    const content = await page.content();
    return content;
  } catch (error) {
    console.error('Browser scraping failed:', error);
    throw error;
  } finally {
    if (browser) await browser.close();
  }
}

// Main scraping function with multiple fallback strategies
async function scrapePage(url, pageNumber, attempt = 1) {
  try {
    // First try with axios and headers
    console.log(`Attempt ${attempt}: Scraping with axios - ${url}`);
    const response = await axios.get(url, {
      headers: getRequestHeaders(pageNumber),
      timeout: 20000,
      proxy: process.env.PROXY_URL ? {
        protocol: 'http',
        host: process.env.PROXY_URL,
        port: process.env.PROXY_PORT || 80
      } : undefined
    });

    return response.data;
  } catch (error) {
    if (attempt >= 3) {
      // Final fallback to browser
      console.log(`Falling back to browser scraping for ${url}`);
      return await scrapeWithBrowser(url);
    }
    
    // Exponential backoff
    const delay = Math.min(1000 * Math.pow(2, attempt), 30000);
    console.log(`Waiting ${delay}ms before retry...`);
    await new Promise(resolve => setTimeout(resolve, delay));
    
    return await scrapePage(url, pageNumber, attempt + 1);
  }
}

async function scrapePages(baseUrl, startPage, endPage) {
  const allBusinesses = [];
  const errors = [];
  const normalizedBaseUrl = baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`;

  for (let page = startPage; page <= endPage; page++) {
    const url = `${normalizedBaseUrl}${page}`;
    
    try {
      console.log(`Scraping page ${page}: ${url}`);
      const html = await scrapePage(url, page);
      const $ = cheerio.load(html);
      
      $('.company.with_img').each((index, element) => {
        if ($(element).hasClass('company_ad')) return;
        
        const name = $(element).find('.company_header h3 a').text().trim();
        const address = $(element).find('.company_header .address').text().trim();
        
        // Phone number
        let phone = '';
        const phoneElement = $(element).find('.cont .s:has(.fa-phone) span');
        if (phoneElement.length) phone = phoneElement.text().trim();

        // Image URL
        let imageUrl = '';
        const imageElement = $(element).find('.logo.lazy-img');
        if (imageElement.length) {
          imageUrl = imageElement.attr('data-bg') || 
                    imageElement.css('background-image').replace(/url\(['"]?(.*?)['"]?\)/i, '$1');
        }

        // Verified status
        const verified = $(element).find('.cont u.v:has(.fa-check-circle)').length > 0;
        
        allBusinesses.push({
          name,
          address,
          phone,
          imageUrl: imageUrl ? new URL(imageUrl, 'https://www.businesslist.com.ng').toString() : null,
          verified,
          sourcePage: page
        });
      });

      // Random delay between 5-10 seconds
      const delay = Math.floor(Math.random() * 5000) + 5000;
      await new Promise(resolve => setTimeout(resolve, delay));
      
    } catch (error) {
      errors.push({ page, url, error: error.message });
      console.error(`Error scraping page ${page}:`, error.message);
      
      // Longer delay on error
      await new Promise(resolve => setTimeout(resolve, 15000));
    }
  }

  // Generate CSV only if we have data
  if (allBusinesses.length > 0) {
    const csvData = stringify(allBusinesses, {
      header: true,
      columns: [
        { key: 'name', header: 'Business Name' },
        { key: 'address', header: 'Address' },
        { key: 'phone', header: 'Phone Number' },
        { key: 'imageUrl', header: 'Image URL' },
        { key: 'verified', header: 'Verified' },
        { key: 'sourcePage', header: 'Page Number' }
      ]
    });

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `businesses_${timestamp}.csv`;
    const filePath = path.join(outputDir, filename);

    fs.writeFileSync(filePath, csvData);
    console.log(`CSV saved to: ${filePath}`);

    return {
      businesses: allBusinesses,
      errors,
      stats: {
        totalPagesAttempted: endPage - startPage + 1,
        successfulPages: (endPage - startPage + 1) - errors.length,
        failedPages: errors.length,
        businessesScraped: allBusinesses.length,
        csvFile: filename
      }
    };
  } else {
    throw new Error('No businesses were scraped. All attempts failed.');
  }
}

// API Endpoints
app.post('/scrape', async (req, res) => {
  const { baseUrl, startPage, endPage } = req.body;
  
  // Validation
  if (!baseUrl || startPage === undefined || endPage === undefined) {
    return res.status(400).json({ 
      error: 'Missing parameters',
      details: 'Provide baseUrl, startPage, and endPage'
    });
  }
  
  if (startPage < 1 || endPage < 1 || startPage > endPage) {
    return res.status(400).json({ 
      error: 'Invalid page range',
      details: 'Page numbers must be positive and startPage ≤ endPage'
    });
  }
  
  try {
    const result = await scrapePages(baseUrl, startPage, endPage);
    res.json({ 
      success: true,
      ...result,
      downloadLink: `/download/${result.stats.csvFile}`
    });
  } catch (error) {
    res.status(500).json({ 
      success: false,
      error: error.message,
      solution: 'The website is blocking our requests. Consider: 1) Using a proxy service, 2) Trying during off-peak hours, 3) Implementing CAPTCHA solving if needed.'
    });
  }
});

// Download endpoint
app.get('/download/:filename', (req, res) => {
  const filePath = path.join(outputDir, req.params.filename);
  if (fs.existsSync(filePath)) {
    res.download(filePath);
  } else {
    res.status(404).json({ error: 'File not found' });
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
  console.log(`CSV files will be saved in: ${path.join(__dirname, 'output')}`);
  console.log(`Configure proxy in .env file if needed`);
});