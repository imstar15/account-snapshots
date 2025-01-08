require('dotenv').config();
const _ = require("lodash");
const pgp = require('pg-promise')();

async function fetchBalances(client, blockNumber) {
  const query = `
      SELECT DISTINCT ON (account_id) account_id, total_balance
      FROM turing.account_snapshots
      WHERE snapshot_at_block <= $1
      ORDER BY account_id, snapshot_at_block DESC;
    `;

  const res = await client.query(query, [blockNumber]);
  return res.rows;
}

// node src/index.js 6277426
// node src/index.js 6242226,6277426
// node src/index.js 6205000,6242226,6277426

const main = async () => {
  if (_.isEmpty(process.env.SUBQL_PG_URL)) {
    throw new Error("SUBQL_PG_URL environment variable is not set");
  }

  if (_.isEmpty(process.env.ACCOUNT_SNAPSHOTS_PG_URL)) {
    throw new Error("ACCOUNT_SNAPSHOTS_PG_URL environment variable is not set");
  }

  // Read block numbers from command line
  const blockNumbersLine = process.argv[2];
  const blockNumbers = _.split(blockNumbersLine, ",");
  if (blockNumbers.length === 0) {
    throw new Error("No block numbers are provided");
  }

  const sortedBlockNumbers = _.sortBy(blockNumbers, (blockNumber) => blockNumber);
  const balanceColumnNames = _.map(sortedBlockNumbers, (blockNumber) => `balance_at_${blockNumber}`);
  
  const dbSubQL = pgp(process.env.SUBQL_PG_URL);
  console.log("Fetching balances for block numbers: ", sortedBlockNumbers);
  const results = await Promise.all(_.map(sortedBlockNumbers, (blockNumber) => fetchBalances(dbSubQL, blockNumber)));
  console.log("Finished fetching balances");
  
  const accountBalances = _.clone(_.last(results));
  // Merge the first n-1 results into the accountBalances with the block number as the key
  console.log("Merging results......");
  _.each(accountBalances, (item) => {
    _.each(_.slice(results, 0, -1), ({ account_id, total_balance }, index) => {
      if (item.account_id === account_id) {
        item[balanceColumnNames[index]] = total_balance;
      }
    })
    item[balanceColumnNames[balanceColumnNames.length-1]] = total_balance;
    delete item.total_balance;
  })

  console.log("accountBalances: ", accountBalances);

  // Create a new table
  const tableName = `account_snapshots_${Date.now()}`;
  const colNames = ['account_id', ...balanceColumnNames];
  const dbWrite = pgp(process.env.SNAPSHOT_PG_URL);
  const fields =  colNames.map((colName) => `${colName} TEXT`).join(", ");
  const createTableQuery = `CREATE TABLE IF NOT EXISTS ${tableName} (${fields});`;
  console.log(`Creating table by SQL: `, createTableQuery);
  await dbWrite.query(createTableQuery);
  console.log(`Created table ${tableName}`);

  // Batch insert data
  console.log("Inserting data into table: ", tableName);
  const placeholders = values.map((_, i) => `$${i + 1}`).join(", ");
  const insertQuery = `INSERT INTO ${tableName} (${fields}) VALUES (${placeholders})`;
  await dbWrite.tx(t => {
    const queries = _.map(accountBalances, row => t.none(insertQuery, row));
    return t.batch(queries);
  });
  console.log("Inserted data into table: ", tableName);
};

main().catch(console.error);
