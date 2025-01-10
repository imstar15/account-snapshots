const _ = require('lodash');
const pgp = require('pg-promise')();
require('dotenv').config();

const getAccountCountOnChain = async (db, chain, date) => {
  const query = `SELECT COUNT(id) FROM account_${date} WHERE chain=$1;`
  const result = await db.one(query, [chain]);
  return result.count;
}

const getTotalBalanceOnChain = async (db, chain, date) => {
  const query = `SELECT SUM(balance) FROM account_${date} WHERE chain=$1;`
  const result = await db.one(query, [chain]);
  return result.sum;
}

const main = async() => {
  if (_.isEmpty(process.env.ACCOUNT_SNAPSHOTS_PG_URL)) {
    throw new Error("ACCOUNT_SNAPSHOTS_PG_URL environment variable is not set");
  }

  let date = process.argv[2];
  if (_.isEmpty(date)) {
    throw new Error("Please provide a date");
  }

  const dbSnapshot = pgp(process.env.ACCOUNT_SNAPSHOTS_PG_URL);

  const accountCountOnTuring = await getAccountCountOnChain(dbSnapshot, "turing", date);
  const accountCountOnMangata = await getAccountCountOnChain(dbSnapshot, "mangata", date);
  const totalBalanceOnTuring = await getTotalBalanceOnChain(dbSnapshot, "turing", date);
  const totalBalanceOnMangata = await getTotalBalanceOnChain(dbSnapshot, "mangata", date);
  
  console.log("The number of TUR wallets on Turing: ", accountCountOnTuring);
  console.log("The number of TUR wallets on Mangata: ", accountCountOnMangata);
  console.log("The total amount of TUR on Turing: ", totalBalanceOnTuring);
  console.log("The total amount of TUR on Mangata: ", totalBalanceOnMangata);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});