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

## Calculate the number of TUR wallets and the total amount at the given date

```
yarn run total 0110
```

Output:
```
The number of TUR wallets on Turing:  11016
The number of TUR wallets on Mangata:  676
The total amount of TUR on Turing:  8065126816099489895
The total amount of TUR on Mangata:  17932575313360301
```

You can run the SQL yourself to get the result.

```
SELECT COUNT(id) FROM account_0109 WHERE chain='turing';
SELECT COUNT(id) FROM account_0109 WHERE chain='mangata';
SELECT SUM(balance) FROM account_0109 WHERE chain='turing';
SELECT SUM(balance) FROM account_0109 WHERE chain='mangata';
```
