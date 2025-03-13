require('dotenv').config();
const _ = require("lodash");
const pgp = require('pg-promise')();
const moment = require('moment');
const inquirer = require('inquirer').default;
const Decimal = require('decimal.js');

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const getTableName = (currentDate) => `account_${currentDate}`;

const fetchTokenHoldersOnTuring = async (subscanApiKey) => {
  let accounts = [];
  let page = 0;
  let maxPage = 1;
  console.log("Fetching token holders on Turing...");
  do {
    console.log("fetching token holders, page: ", page);
    const resp = await fetch("https://turing.api.subscan.io/api/v2/scan/accounts", {
      method: 'POST',
      headers: {
      'Content-Type': 'application/json',
      'x-api-key': subscanApiKey,
      },
      body: JSON.stringify({ row: 100, page })
    })
    const result = await resp.json();
    const count = result.data.count;
    maxPage = Math.ceil(count / 100);
    const addresses = _.map(result.data.list, (data) => ({ address: data.address, balance: new Decimal(data.balance).times(10000000000).toString(), data }));
    accounts = _.concat(accounts, addresses);
    await delay(1000);
    page += 1;
  } while (page < maxPage);

  console.log("Fetched token holders on Turing, length: ", accounts.length);
  return accounts;
};

const fetchTokenHoldersOnMangata = async (subscanApiKey) => {
  let accounts = [];
  let page = 0;
  let maxPage = 1;
  console.log("Fetching token holders on Mangata...");
  do {
    console.log("fetching token holders, page: ", page);
    const resp = await fetch("https://mangatax.api.subscan.io/api/scan/token/holders", {
      method: 'POST',
      headers: {
      'Content-Type': 'application/json',
      'x-api-key': subscanApiKey,
      },
      body: JSON.stringify({ unique_id: "7", row: 100, page })
    })
    const result = await resp.json();
    const count = result.data.count;
    maxPage = Math.ceil(count / 100);
    const addresses = _.map(result.data.list, (data) => ({ address: data.account_display.address, balance: data.balance, data }));
    accounts = _.concat(accounts, addresses);
    await delay(1000);
    page += 1;
  } while (page < maxPage);

  console.log("Fetched token holders on Mangata, length: ", accounts.length);
  return accounts;
};

/**
 * Check if we need to calculate the account table for the given date
 * If the table exists, ask the user if they want to recalculate
 * @param {*} db 
 * @param {*} currentDate 
 * @returns 
 */
const checkIfNeedToCalculate = async (db, currentDate) => {
  const res = await db.one(`
    SELECT EXISTS (
      SELECT FROM information_schema.tables 
      WHERE table_name = $1
    );
  `, [getTableName(currentDate)]);

  if (res.exists) {
    console.log(`The account table for date ${currentDate} exists.`);
    const { userInput } = await inquirer.prompt([
      {
        type: 'input',
        name: 'userInput',
        message: 'Type YES to recalculate, or press Enter to skip',
      },
    ]);
    return _.toUpper(userInput) === 'YES';
  }

  return true;
};

const main = async () => {
  if (_.isEmpty(process.env.SUBQL_PG_URL)) {
    throw new Error("SUBQL_PG_URL environment variable is not set");
  }

  if (_.isEmpty(process.env.ACCOUNT_SNAPSHOTS_PG_URL)) {
    throw new Error("ACCOUNT_SNAPSHOTS_PG_URL environment variable is not set");
  }

  if (_.isEmpty(process.env.SUBSCAN_API_KEY)) {
    throw new Error("SUBSCAN_API_KEY environment variable is not set");
}

  const currentDate = moment().format('MMDD');
  const dbSnapshot = pgp(process.env.ACCOUNT_SNAPSHOTS_PG_URL);

  const isNeedCalculation = await checkIfNeedToCalculate(dbSnapshot, currentDate);
  if (!isNeedCalculation) {
    return;
  }

  // Fetch balances on Turing
  const tokenHoldersOnTuring = await fetchTokenHoldersOnTuring(process.env.SUBSCAN_API_KEY);

  // Fetch token holders on Mangata
  const tokenHoldersOnMangata = await fetchTokenHoldersOnMangata(process.env.SUBSCAN_API_KEY);

  // Create a new table
  const tableName = getTableName(currentDate);
  await dbSnapshot.query(`DROP TABLE IF EXISTS ${tableName};`);
  const createTableQuery = `CREATE TABLE ${tableName} (id SERIAL PRIMARY KEY, chain TEXT NOT NULL, address TEXT NOT NULL, balance NUMERIC NOT NULL, data JSONB NOT NULL);`;
  console.log(`Creating table by SQL: `, createTableQuery);
  await dbSnapshot.query(createTableQuery);
  console.log(`Created table ${tableName}`);

  // Batch insert data
  console.log("Inserting data into table: ", tableName);
  let insertQuery = `INSERT INTO ${tableName} (chain, address, balance, data) VALUES `;
  
  // Insert balances on Turing
  insertQuery += _.join(_.map(tokenHoldersOnTuring, (row) => `('turing', '${row.address}', ${row.balance}, '${JSON.stringify(row.data).replace(/'/g, "''")}'::jsonb)`), ', ');

  // Insert token holders on Mangata
  if (tokenHoldersOnMangata.length > 0) {
    insertQuery += ', ';
    insertQuery += _.join(_.map(tokenHoldersOnMangata, (row) => `('mangata', '${row.address}', ${row.balance}, '${JSON.stringify(row.data).replace(/'/g, "''")}'::jsonb)`), ', ');
  }

  await dbSnapshot.query(insertQuery);
  console.log("Inserted data into table: ", tableName);
};

main().catch(console.error);
