require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cheerio = require('cheerio');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const { stringify } = require('csv-stringify/sync');
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

async function scrapePages(baseUrl, startPage, endPage) {
  const allBusinesses = [];
  const errors = [];
  const normalizedBaseUrl = baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`;

  for (let page = startPage; page <= endPage; page++) {
    const url = `${normalizedBaseUrl}${page}`;
    
    try {
      console.log(`Scraping page ${page}: ${url}`);
      const response = await axios.get(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        },
        timeout: 10000
      });

      const $ = cheerio.load(response.data);
      
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

      await new Promise(resolve => setTimeout(resolve, 2000));
      
    } catch (error) {
      errors.push({ page, url, error: error.message });
      console.error(`Error scraping page ${page}:`, error.message);
    }
  }

  // Generate CSV
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

  // Create filename with timestamp
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const filename = `businesses_${timestamp}.csv`;
  const filePath = path.join(outputDir, filename);

  // Save to CSV
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
      error: error.message 
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
});