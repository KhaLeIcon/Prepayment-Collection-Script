const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');
const xlsx = require('xlsx');
const axios = require('axios');
const https = require('https');
const pLimitModule = require('p-limit');
const pLimit = pLimitModule.default || pLimitModule;

// Load configuration
const isPkg = typeof process.pkg !== 'undefined';
const baseDir = isPkg
    // when running as .exe, resolve to the folder containing the exe
    ? path.dirname(process.execPath)
    // in dev, resolve to project root
    : path.join(__dirname, '..');

// ---------- Speed: shared axios client with KeepÃ¢â‚¬'Alive ----------
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 40 });
const http = axios.create({
  timeout: 30000,
  httpsAgent,
  headers: { Prefer: 'odata.maxpagesize=500' } // harmless on V2, helpful on V4
});

// ---------- Concurrency pools (tune as needed) ----------
const soItemLimit = pLimit(12);
const acctLimit   = pLimit(12);
const flagLimit   = pLimit(12);

// Fetch all records from a paginated API endpoint
async function fetchAllRecords(baseUrl, auth, params) {
    const all = [];

    // first page
    let resp = await http.get(baseUrl, { auth, params });
    let data = resp.data;
    if (Array.isArray(data.value)) all.push(...data.value);

    // V4 next link
    let nextLink = data['@odata.nextLink'];

    // V2 fallback
    if (!nextLink && data?.d?.__next) {
      nextLink = data.d.__next;
      if (Array.isArray(data.d?.results)) all.push(...data.d.results);
    }

    // follow next links (V4 or V2)
    while (nextLink) {
        const pageUrl = new URL(nextLink, baseUrl).href;
        resp = await http.get(pageUrl, { auth });
        data = resp.data;

        if (Array.isArray(data.value)) {
          all.push(...data.value);
          nextLink = data['@odata.nextLink'];
        } else if (Array.isArray(data?.d?.results)) {
          all.push(...data.d.results);
          nextLink = data.d.__next || null;
        } else {
          nextLink = null;
        }
    }
    return all;
}

// Process company codes with scenario B
async function processScenarioB(companyCodeRows, config, auth, companyCodeConfigMap) {
    console.log(`Processing ${companyCodeRows.length} company codes with Scenario B`);
    
    const {
        ScenarioB: scenarioBUrlPath,
        GetSalesOrderB: getSalesOrderBUrlPath,
        FilterSalesOrderItem: itemUrlPath,
        Flag: flagUrlPath
    } = config.cpi.endpoints;

    const { hostname } = auth.credentials;
    const scenarioBUrl = hostname + scenarioBUrlPath;
    const getSalesOrderBUrl = hostname + getSalesOrderBUrlPath;
    const itemUrl = hostname + itemUrlPath;
    const flagUrl = hostname + flagUrlPath;
    
    // Create debug folder
    const debugFolder = path.join(baseDir, 'debug');
    fs.mkdirSync(debugFolder, { recursive: true });
    
    // Collect data for each step
    const step1Data = [];
    const step2Data = [];
    const scenarioBResults = [];
    
    for (const row of companyCodeRows) {
        const { CompanyCode } = row;
        console.log(`Processing Scenario B for CompanyCode: ${CompanyCode}`);
        
        try {
            // Step 1: Call ScenarioB URL with filters
            const scenarioBResp = await http.get(scenarioBUrl, {
                auth: auth.auth,
                params: {
                    $filter: `SalesOrganization eq '${CompanyCode}' and YY1_PrepaymentScenario_BDH eq 'B' and InvoiceClearingStatus eq 'C'`,
                    $select: 'InvoiceClearingStatus,BillingDocument,YY1_PrepaymentScenario_BDH'
                }
            });
            
            const scenarioBRecords = scenarioBResp.data?.d?.results || [];
            step1Data.push(...scenarioBRecords.map(record => ({ ...record, CompanyCode })));
            console.log(`Found ${scenarioBRecords.length} records from ScenarioB API for CompanyCode: ${CompanyCode}`);
            
            // Step 2: For each BillingDocument, call GetSalesOrderB URL
            const getSalesOrderBResults = await Promise.all(
                scenarioBRecords.map(record =>
                    flagLimit(async () => {
                        const { BillingDocument } = record;
                        if (!BillingDocument) return null;
                        
                        try {
                            const getSalesOrderBResp = await http.get(getSalesOrderBUrl, {
                                auth: auth.auth,
                                params: {
                                    $filter: `ReferenceDocument eq '${BillingDocument}'`,
                                    $select: 'AccountingDocument,FiscalYear,SalesDocument,SalesDocumentItem'
                                }
                            });
                            
                            const getSalesOrderBRecords = getSalesOrderBResp.data?.d?.results || [];

                            const validRecords = getSalesOrderBRecords.filter(fr => 
                                fr.SalesDocument && fr.SalesDocument.trim() !== ''
                            );

                            return validRecords.map(fr => ({
                                CompanyCode,
                                BillingDocument,
                                AccountingDocument: fr.AccountingDocument,
                                FiscalYear: fr.FiscalYear,
                                SalesDocument: fr.SalesDocument,
                                SalesDocumentItem: fr.SalesDocumentItem
                            }));
                            
                        } catch (error) {
                            console.error(`Error processing BillingDocument ${BillingDocument} for CompanyCode ${CompanyCode}:`, error.message);
                            return null;
                        }
                    })
                )
            );
            
            // Flatten results and filter out nulls
            const flatResults = getSalesOrderBResults.filter(Boolean).flat();
            step2Data.push(...flatResults);
            
            // Step 3: For each unique SalesOrder, get YY1_SALESFORCEID_I_SDI from items
            const uniqueSalesOrders = [...new Set(flatResults.map(r => r.SalesDocument))];
            const salesOrderItemsMap = new Map();
            
            await Promise.all(
                uniqueSalesOrders.map(so =>
                    soItemLimit(async () => {
                        if (!so) return;
                        
                        try {
                            const itemResp = await http.get(itemUrl, {
                                auth: auth.auth,
                                params: {
                                    $filter: `SalesOrder eq '${so}'`,
                                    $select: 'SalesOrderItem,YY1_SALESFORCEID_I_SDI'
                                }
                            });

                            const itemRecords = itemResp.data?.value || [];
                            
                            // Create a map of SalesOrderItem -> YY1_SALESFORCEID_I_SDI for this SO
                            const itemMap = {};
                            itemRecords.forEach(item => {
                                if (item.SalesOrderItem && item.YY1_SALESFORCEID_I_SDI) {
                                    itemMap[item.SalesOrderItem] = item.YY1_SALESFORCEID_I_SDI;
                                }
                            });
                            
                            salesOrderItemsMap.set(so, itemMap);
                            
                        } catch (error) {
                            console.error(`Error getting items for SalesOrder ${so}:`, error.message);
                        }
                    })
                )
            );
            
            // Transform to final format with YY1_SALESFORCEID_I_SDI
            const transformedResults = flatResults.map(result => {
                const itemMap = salesOrderItemsMap.get(result.SalesDocument);
                const salesforceId = itemMap ? itemMap[result.SalesDocumentItem] : null;
                
                return {
                    CompanyCode: result.CompanyCode,
                    SalesOrder: result.SalesDocument,
                    Customer: null, // Will be populated in future API calls
                    SalesOrderItem: {
                        SalesOrderItem: result.SalesDocumentItem,
                        YY1_SALESFORCEID_I_SDI: salesforceId
                    },
                    AccountingDocument: result.AccountingDocument,
                    FiscalYear: result.FiscalYear,
                    OriginalBillingDocument: result.BillingDocument // Keep track of original billing document
                };
            });
            
            scenarioBResults.push(...transformedResults);
            
            console.log(`Processed ${transformedResults.length} records for CompanyCode: ${CompanyCode}`);
            
        } catch (error) {
            console.error(`Error processing Scenario B for CompanyCode ${CompanyCode}:`, error.message);
        }
    }
    
    // Save Step 1: ScenarioB URL results
    fs.writeFileSync(
        path.join(debugFolder, 'step1_B.json'),
        JSON.stringify(step1Data, null, 2),
        'utf8'
    );
    console.log(`Saved step1_B.json with ${step1Data.length} records`);
    
    // Save Step 2: GetSalesOrderB URL results
    fs.writeFileSync(
        path.join(debugFolder, 'step2_B.json'),
        JSON.stringify(step2Data, null, 2),
        'utf8'
    );
    console.log(`Saved step2_B.json with ${step2Data.length} records`);
    
    // Save Step 3: After itemUrl calls (with YY1_SALESFORCEID_I_SDI)
    fs.writeFileSync(
        path.join(debugFolder, 'step3_B.json'),
        JSON.stringify(scenarioBResults, null, 2),
        'utf8'
    );
    console.log(`Saved step3_B.json with ${scenarioBResults.length} records`);
    
    // --------- Apply flag filtering to Scenario B results ----------
    const disallowedStatuses = ["Paid", "Sent", "Error"];
    const flaggedScenarioBResults = await Promise.all(
        scenarioBResults.map(r =>
            flagLimit(async () => {
                if (!r.AccountingDocument) return null;

                try {
                    const resp = await http.get(flagUrl, {
                        auth: auth.auth,
                        params: {
                            $filter: `AccountingDocument eq '${r.AccountingDocument}' and CompanyCode eq '${r.CompanyCode}'`
                        }
                    });

                    const fr = resp.data?.d?.results?.[0] || {};
                    const isDisallowedStatus = disallowedStatuses.includes(fr.Statuscode);
                    
                    // Get CheckFlagNA setting for this company code
                    const companyConfig = companyCodeConfigMap.get(r.CompanyCode);
                    const checkFlagNA = companyConfig?.CheckFlagNA === 'Yes';
                    
                    // Original flag check logic
                    let isAnyFlagYes = fr.FlagSFUpdated === 'Yes' || fr.FlagInvoiceSent === 'Yes';
                    
                    // If CheckFlagNA is 'Yes' for this company code, also check for 'NA' values
                    if (checkFlagNA) {
                        isAnyFlagYes = isAnyFlagYes || fr.FlagSFUpdated === 'NA' || fr.FlagInvoiceSent === 'NA';
                    }
                    
                    const exclude = isDisallowedStatus || isAnyFlagYes;

                    return exclude ? null : r;
                } catch (error) {
                    console.error(`Error checking flags for AccountingDocument ${r.AccountingDocument} in CompanyCode ${r.CompanyCode}:`, error.message);
                    return null;
                }
            })
        )
    );
    
    const filteredScenarioBResults = flaggedScenarioBResults.filter(Boolean);
    
    // Save Step 4: After flag filtering
    fs.writeFileSync(
        path.join(debugFolder, 'step4_B.json'),
        JSON.stringify(filteredScenarioBResults, null, 2),
        'utf8'
    );
    console.log(`Saved step4_B.json with ${filteredScenarioBResults.length} records`);
    
    console.log(`Total Scenario B results after flag filtering: ${filteredScenarioBResults.length}`);
    return filteredScenarioBResults;
}

// Normal processing for non-B scenarios
async function processNormalScenario(companyCodeRows, config, auth, companyCodeConfigMap) {
    const {
        FilterSalesOrderHeader: headerUrlPath,
        FilterSalesOrderItem: itemUrlPath,
        GetAccountingDocument: acctUrlPath,
        Flag: flagUrlPath
    } = config.cpi.endpoints;

    const { hostname } = auth.credentials;
    const headerUrl = hostname + headerUrlPath;
    const itemUrl = hostname + itemUrlPath;
    const acctUrl = hostname + acctUrlPath;
    const flagUrl = hostname + flagUrlPath;

    // Create debug folder
    const debugFolder = path.join(baseDir, 'debug');
    fs.mkdirSync(debugFolder, { recursive: true });

    const filterOutSO = Array.isArray(config.filteroutSO)
        ? config.filteroutSO.map(String)
        : [];

    // Collect data for each step
    const step1Data = [];
    const finalList = [];
    
    // Get SO for each CompanyCode
    for (const row of companyCodeRows) {
        const { CompanyCode } = row;
        console.log(`Processing Normal Scenario for CompanyCode: ${CompanyCode}`);
        if (!CompanyCode) continue;

        const headers = await fetchAllRecords(headerUrl, auth.auth, { $filter: `SalesOrganization eq '${CompanyCode}'` });
        step1Data.push(...headers.map(h => ({ ...h, CompanyCode })));
        
        const validHeaders = headers.filter(r =>
            r?.YY1_PrepaymentScenario_SDH?.toString().trim() &&
            r?.SalesOrder &&
            !filterOutSO.includes(String(r.SalesOrder))
        );

        // --------- Bounded parallel fetch of items per SO ----------
        const rawItemDetails = [];
        await Promise.all(
          validHeaders.map(h =>
            soItemLimit(async () => {
              const so = h.SalesOrder;
              const customer = h.SoldToParty;

              const items = await fetchAllRecords(itemUrl, auth.auth, { $filter: `SalesOrder eq '${so}'` });
              rawItemDetails.push({ CompanyCode, SalesOrder: so, RawItems: items });

              const list = items
                .filter(it => it.SlsOrderItemDownPaymentStatus === 'D')
                .map(it => ({ SalesOrderItem: it.SalesOrderItem, YY1_SALESFORCEID_I_SDI: it.YY1_SALESFORCEID_I_SDI }))
                .filter(Boolean);

              if (list.length) {
                finalList.push({ CompanyCode, SalesOrder: so, Customer: customer, SalesOrderItems: list });
              }
            })
          )
        );
    }

    // Save Step 1: Header URL results
    fs.writeFileSync(
        path.join(debugFolder, 'step1_notB.json'),
        JSON.stringify(step1Data, null, 2),
        'utf8'
    );
    console.log(`Saved step1_notB.json with ${step1Data.length} records`);

    // Save Step 2: Item URL results (filtered items with status 'D')
    fs.writeFileSync(
        path.join(debugFolder, 'step2_notB.json'),
        JSON.stringify(finalList, null, 2),
        'utf8'
    );
    console.log(`Saved step2_notB.json with ${finalList.length} sales orders`);

    // --------- Bounded parallel: Get Accounting Document ----------
    const processedResults = await Promise.all(
      finalList.flatMap(({ CompanyCode, SalesOrder, Customer, SalesOrderItems }) =>
        SalesOrderItems.map(item =>
          acctLimit(async () => {
            const resp = await http.get(acctUrl, {
                auth: auth.auth,
                params: {
                    $filter: `SalesDocument eq '${SalesOrder}' and SalesDocumentItem eq '${item.SalesOrderItem}'`,
                    $select: 'AccountingDocument,AccountingDocumentItem,AmountInTransactionCurrency'
                }
            });
            const results = resp.data?.d?.results || [];

            if (results.length !== 2) {
              console.warn(`Expected 2 items but got ${results.length} for SalesOrder: ${SalesOrder}, Item: ${item.SalesOrderItem}`);
            }
            
            //Change in logic, instead of using the one with Accounting Document Item = 2,
            //Nikhil decide that we take the AmountInTransactionCurrency <0
            const rec = results.find(r => r.AmountInTransactionCurrency < 0);
            
            const totalAmount = results.reduce((sum, r) => sum + (r.AmountInTransactionCurrency || 0), 0);
            if (Math.abs(totalAmount) > 0.01) { // Using small threshold for floating point comparison
              console.warn(`Sum is not zero: ${totalAmount} for SalesOrder: ${SalesOrder}, Item: ${item.SalesOrderItem}`);
            }

            const AccountingDocument = rec?.AccountingDocument || null;
            const idStr = rec?.__metadata?.id || '';
            const fyMatch = idStr.match(/FiscalYear='(\d+)'/);
            const FiscalYear = fyMatch ? fyMatch[1] : null;
            return { CompanyCode, SalesOrder, Customer, SalesOrderItem: item, AccountingDocument, FiscalYear };
          })
        )
      )
    );

    // Save Step 3: Accounting Document URL results
    fs.writeFileSync(
        path.join(debugFolder, 'step3_notB.json'),
        JSON.stringify(processedResults, null, 2),
        'utf8'
    );
    console.log(`Saved step3_notB.json with ${processedResults.length} records`);

    // --------- Bounded parallel: Filter SO by Flag ----------
    const disallowedStatuses = ["Paid", "Sent", "Error"];
    const flagged = await Promise.all(
      processedResults.map(r =>
        flagLimit(async () => {
          if (!r.AccountingDocument) return null;

          const resp = await http.get(flagUrl, {
              auth: auth.auth,
              params: {
                  $filter: `AccountingDocument eq '${r.AccountingDocument}' and CompanyCode eq '${r.CompanyCode}'`
              }
          });

          const fr = resp.data?.d?.results?.[0] || {};
          const isDisallowedStatus = disallowedStatuses.includes(fr.Statuscode);
          
          // Get CheckFlagNA setting for this company code
          const companyConfig = companyCodeConfigMap.get(r.CompanyCode);
          const checkFlagNA = companyConfig?.CheckFlagNA === 'Yes';
          
          // Original flag check logic
          let isAnyFlagYes = fr.FlagSFUpdated === 'Yes' || fr.FlagInvoiceSent === 'Yes';
          
          // If CheckFlagNA is 'Yes' for this company code, also check for 'NA' values
          if (checkFlagNA) {
            isAnyFlagYes = isAnyFlagYes || fr.FlagSFUpdated === 'NA' || fr.FlagInvoiceSent === 'NA';
          }
          
          const exclude = isDisallowedStatus || isAnyFlagYes;

          return exclude ? null : r;
        })
      )
    );
    
    const filteredNormalResults = flagged.filter(Boolean);
    
    // Save Step 4: Flag URL results (after filtering)
    fs.writeFileSync(
        path.join(debugFolder, 'step4_notB.json'),
        JSON.stringify(filteredNormalResults, null, 2),
        'utf8'
    );
    console.log(`Saved step4_notB.json with ${filteredNormalResults.length} records`);
    
    return filteredNormalResults;
}

async function main() {
    // load config.yaml
    const configPath = path.join(baseDir, 'config.yaml');
    const config = yaml.load(fs.readFileSync(configPath, 'utf8'));

    const envKey = (config.env || '').toLowerCase();
    const creds = config.credentials[envKey];
    if (!creds) throw new Error(`Missing credentials for env ${config.env}`);

    const { username, password, hostname } = creds;
    const {
        FilterSalesOrderHeader: headerUrlPath,
        FilterSalesOrderItem: itemUrlPath,
        GetAccountingDocument: acctUrlPath,
        Flag: flagUrlPath,
        ScenarioB: scenarioBUrlPath,
        GetSalesOrderB: getSalesOrderBUrlPath
    } = config.cpi.endpoints;

    const outputFolder = config.outputfolder || path.join(baseDir, 'output');

    if (!headerUrlPath || !itemUrlPath || !acctUrlPath || !flagUrlPath || !scenarioBUrlPath || !getSalesOrderBUrlPath) {
        throw new Error('Missing one or more CPI endpoints in config.yaml');
    }

    const authConfig = {
        auth: { username, password },
        credentials: { hostname }
    };

    //Read Workbook
    const workbookPath = path.join(baseDir, 'CompanyCodeList.xlsx');
    const workbook = xlsx.readFile(workbookPath);
    const sheetName = workbook.SheetNames[0];
    const rows = xlsx.utils.sheet_to_json(workbook.Sheets[sheetName]);

    // Create a map for quick lookup of company code configurations
    const companyCodeConfigMap = new Map();
    rows.forEach(row => {
        if (row.CompanyCode) {
            companyCodeConfigMap.set(row.CompanyCode, {
                Scenario: row.Scenario,
                CheckFlagNA: row.CheckFlagNA
            });
        }
    });

    // Create debug folder
    const debugFolder = path.join(baseDir, 'debug');
    fs.mkdirSync(debugFolder, { recursive: true });

    // Separate company codes by scenario
    const scenarioBRows = rows.filter(row => row.Scenario === 'B');
    const normalScenarioRows = rows.filter(row => row.Scenario !== 'B');

    console.log(`Found ${scenarioBRows.length} company codes with Scenario B`);
    console.log(`Found ${normalScenarioRows.length} company codes with normal scenarios`);

    // Process both scenarios
    const [normalResults, scenarioBResults] = await Promise.all([
        normalScenarioRows.length > 0 ? processNormalScenario(normalScenarioRows, config, authConfig, companyCodeConfigMap) : Promise.resolve([]),
        scenarioBRows.length > 0 ? processScenarioB(scenarioBRows, config, authConfig, companyCodeConfigMap) : Promise.resolve([])
    ]);

    // Combine results from both scenarios
    const combinedFlagResults = [...normalResults, ...scenarioBResults];

    // Save final combined results
    fs.writeFileSync(
        path.join(debugFolder, 'combined.json'),
        JSON.stringify(combinedFlagResults, null, 2),
        'utf8'
    );
    console.log(`Saved combined.json with ${combinedFlagResults.length} records`);

    // Group combined results by CompanyCode
    const grouped = combinedFlagResults.reduce((acc, cur) => {
        (acc[cur.CompanyCode] = acc[cur.CompanyCode] || []).push(cur);
        return acc;
    }, {});

    // Save CSV by CompanyCode using combined results
    const now = new Date();
    const yyyy = now.getFullYear();
    const MM = String(now.getMonth() + 1).padStart(2, '0');
    const dd = String(now.getDate()).padStart(2, '0');
    const HH = String(now.getHours()).padStart(2, '0');
    const mm = String(now.getMinutes()).padStart(2, '0');
    const timestamp = `${yyyy}${MM}${dd}_${HH}${mm}`;

    for (const [companyCode, records] of Object.entries(grouped)) {
        const dir = path.join(outputFolder, companyCode);
        fs.mkdirSync(dir, { recursive: true });
        const filename = `PrePayment_Collection_Invoice_A_${companyCode}_${timestamp}.csv`;
        const fullPath = path.join(dir, filename);
        const header = 'SalesOrder,SalesOrderItem,YY1_SALESFORCEID_I_SDI,Customer,AccountingDocument,CompanyCode,FiscalYear';
        const lines = records.map(r =>
            [r.SalesOrder, r.SalesOrderItem.SalesOrderItem, r.SalesOrderItem.YY1_SALESFORCEID_I_SDI, r.Customer, r.AccountingDocument, r.CompanyCode, r.FiscalYear].join(',')
        );
        fs.writeFileSync(fullPath, [header, ...lines].join('\n'), 'utf8');
        console.log(`Created ${fullPath} with ${records.length} records`);
    }
}

if (require.main === module) {
    main().catch(err => console.error(err));
}
module.exports = main;