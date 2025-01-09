require('dotenv').config();
const _ = require('lodash');
const pgp = require('pg-promise')();
const inquirer = require('inquirer').default;

const TABLE_NAME = "account_avg";

const fetchBalances = async (db, tableName) => {
  console.log("Fetching balances...");
  const query = `SELECT chain, address, balance FROM ${tableName}`;
  const balances = await db.query(query);
  console.log("Fetched balances: ", balances.length);
  return balances;
}

/**
 * Check if we need to calculate the average balance table
 * If the table exists, ask the user if they want to recalculate
 * @param {*} db
 * @returns 
 */
const checkIfNeedToCalculate = async (db) => {
  const res = await db.one(`
    SELECT EXISTS (
      SELECT FROM information_schema.tables 
      WHERE table_name = $1
    );
  `, [TABLE_NAME]);

  if (res.exists) {
    console.log(`The average balance table exists.`);
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

const calculateAverage = (balancesAtDates) => {
  const balances = {};
  _.each(balancesAtDates, (balancesAtDate) => {
    _.each(balancesAtDate, (row) => {
      const { chain, address, balance } = row;
      const key = `${chain}-${address}`;
      if (_.has(balances, key)) {
        balances[key].balances.push(+balance);
      } else {
        balances[key] = {
          chain,
          address,
          balances: [+balance],
        };
      }
    });
  });

  console.log("balances: ", balances["turing-669eD3AneQpvwFQq4jMbYbEMEtLjtKvwQsfkvxBuTMuMswtK"]);

  // Calculate average
  const averages = _.map(balances, (balance) => {
    const avg = _.mean(balance.balances);
    return {
      chain: balance.chain,
      address: balance.address,
      balance: avg,
    };
  });

  return _.values(averages);
};

/**
 * Calculate the average balance of all accounts at the given dates
 * Example: yarn run avg 0107,0108,0109
 */
const main = async() => {
  if (_.isEmpty(process.env.ACCOUNT_SNAPSHOTS_PG_URL)) {
    throw new Error("ACCOUNT_SNAPSHOTS_PG_URL environment variable is not set");
  }

  let dates = _.split(process.argv[2], ",");
  dates = _.sortBy(dates);

  if (dates.length < 2) {
    throw new Error("Please provide at least two dates separated by a comma");
  }

  const dbSnapshot = pgp(process.env.ACCOUNT_SNAPSHOTS_PG_URL);

  // Check if we need to calculate
  const isNeedCalculation = await checkIfNeedToCalculate(dbSnapshot);
  if (!isNeedCalculation) {
    return;
  }

  // Fetch balances
  const balancesAtDates = await Promise.all(dates.map(async (date) => fetchBalances(dbSnapshot, `account_${date}`)));

  // Calculate average
  const averages = calculateAverage(balancesAtDates);

  // Create a new table
  await dbSnapshot.query(`DROP TABLE IF EXISTS ${TABLE_NAME};`);
  const createTableQuery = `CREATE TABLE ${TABLE_NAME} (id SERIAL PRIMARY KEY, chain TEXT NOT NULL, address TEXT NOT NULL, balance NUMERIC NOT NULL);`;
  console.log(`Creating table by SQL: `, createTableQuery);
  await dbSnapshot.query(createTableQuery);
  console.log(`Created table ${TABLE_NAME}`);

  // Save to database
  console.log("Saving averages to database...");
  let insertQuery = `INSERT INTO ${TABLE_NAME} (chain, address, balance) VALUES `;
  insertQuery += _.join(_.map(averages, (row) => `('${row.chain}', '${row.address}', ${row.balance})`), ', ');
  await dbSnapshot.query(insertQuery);
  console.log("Saved averages to database");
};

main().catch(console.error);
