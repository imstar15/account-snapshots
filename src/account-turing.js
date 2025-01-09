require('dotenv').config();
const _ = require("lodash");
const pgp = require('pg-promise')();
const moment = require('moment');
const inquirer = require('inquirer').default;

const getTableName = (currentDate) => `account_${currentDate}_turing`;

async function fetchBalances(db) {
  const query = `
    SELECT DISTINCT ON (account_id) account_id, total_balance
      FROM turing.account_snapshots
      WHERE total_balance != 0
      ORDER BY account_id, snapshot_at_block DESC;
  `;
  const result = await db.query(query);
  return result;
}

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

  const currentDate = moment().format('MMDD');
  const dbSnapshot = pgp(process.env.ACCOUNT_SNAPSHOTS_PG_URL);

  const isNeedCalculation = await checkIfNeedToCalculate(dbSnapshot, currentDate);
  if (!isNeedCalculation) {
    return;
  }

  // Fetch balances
  const dbSubQL = pgp(process.env.SUBQL_PG_URL);
  console.log("Fetching balances...");
  const balances = await fetchBalances(dbSubQL);
  console.log("Fetched balances: ", balances.length);

  // Create a new table
  const tableName = `account_${currentDate}_turing`;
  await dbSnapshot.query(`DROP TABLE IF EXISTS ${tableName};`);
  const createTableQuery = `CREATE TABLE ${tableName} (id SERIAL PRIMARY KEY, account_id TEXT NOT NULL, total_balance NUMERIC NOT NULL);`;
  console.log(`Creating table by SQL: `, createTableQuery);
  await dbSnapshot.query(createTableQuery);
  console.log(`Created table ${tableName}`);

  // Batch insert data
  console.log("Inserting data into table: ", tableName);
  let insertQuery = `INSERT INTO ${tableName} (account_id, total_balance) VALUES `;
  insertQuery += _.join(_.map(balances, (row) => `('${row.account_id}', ${row.total_balance})`), ', ');
  await dbSnapshot.query(insertQuery);
  console.log("Inserted data into table: ", tableName);
};

main().catch(console.error);
