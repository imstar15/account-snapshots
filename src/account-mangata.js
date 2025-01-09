// e0e9a030741440d6bd8e669a57bcf1d3
require('dotenv').config();
const fetch = require('node-fetch-commonjs').default;
const _ = require('lodash');
const moment = require('moment');
const pgp = require('pg-promise')();
const inquirer = require('inquirer').default;

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const fetchAllTokenHolders = async () => {
  let accounts = [];
  let page = 0;
  let maxPage = 1;
  console.log("Fetching token holders...");
  do {
    console.log("fetching token holders, page: ", page);
    const resp = await fetch("https://mangatax.api.subscan.io/api/scan/token/holders", {
      method: 'POST',
      headers: {
      'Content-Type': 'application/json',
      'x-api-key': 'e0e9a030741440d6bd8e669a57bcf1d3'
      },
      body: JSON.stringify({ unique_id: "7", row: 100, page })
    })
    const result = await resp.json();
    const count = result.data.count;
    maxPage = Math.ceil(count / 100);
    const addresses = _.map(result.data.list, ({ account_display: { address }, balance }) => ({ address, balance }));
    accounts = _.concat(accounts, addresses);
    await delay(1000);
    page += 1;
  } while (page < maxPage);
  return accounts;
};

const getTableName = (currentDate) => `account_${currentDate}_mangata`;

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
  if (_.isEmpty(process.env.ACCOUNT_SNAPSHOTS_PG_URL)) {
    throw new Error("ACCOUNT_SNAPSHOTS_PG_URL environment variable is not set");
  }

  const currentDate = moment().format('MMDD');
  const dbSnapshot = pgp(process.env.ACCOUNT_SNAPSHOTS_PG_URL);

  const isNeedCalculation = await checkIfNeedToCalculate(dbSnapshot, currentDate);
  if (!isNeedCalculation) {
    return;
  }

  const allHolders = await fetchAllTokenHolders();
  console.log("allHolders, length: ", allHolders.length);

   // Create a new table
   const tableName = getTableName(currentDate);
   await dbSnapshot.query(`DROP TABLE IF EXISTS ${tableName};`);
   const createTableQuery = `CREATE TABLE ${tableName} (id SERIAL PRIMARY KEY, account_id TEXT NOT NULL, total_balance NUMERIC NOT NULL);`;
   console.log(`Creating table by SQL: `, createTableQuery);
   await dbSnapshot.query(createTableQuery);
   console.log(`Created table ${tableName}`);

   // Batch insert data
  console.log("Inserting data into table: ", tableName);
  let insertQuery = `INSERT INTO ${tableName} (account_id, total_balance) VALUES `;
  insertQuery += _.join(_.map(allHolders, (row) => `('${row.address}', ${row.balance})`), ', ');
  await dbSnapshot.query(insertQuery);
  console.log("Inserted data into table: ", tableName);
};

main().catch(console.error);