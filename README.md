# Account SnapShots

## Preparation

Create a .env file in project root.

```
SUBQL_PG_URL=
ACCOUNT_SNAPSHOTS_PG_URL=
SUBSCAN_API_KEY=
```

And run `yarn`.

## Fetch account balances

```
yarn run accounts
```

It will create a table named `account_<date>` in database.
Every row has the chain, address, balance fileds.


## Calculate the average balance of all accounts at the given dates

```
yarn run avg 0107,0108,0109
```
