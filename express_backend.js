//const bigdecimal = require("bigdecimal");
const {
  chainCoins,
  debank_chain_details,
  debank_protocol_ref,
  COMPOUNDING_TOKENS,
} = require("../constant");

const {
  getCurrentBlockNumber,
  getTokenBalances,
  getTokenTransfers,
  getTransactions,
  getTokenPrice,
  getAssets,
  getDebankValue,
} = require("../helpers/moralis");

const {
  getTokenInfoByDebank,
  getDeBankComplexProtocol,
  getSupplyTokens,
  get_debank_token,
} = require("../helpers/debank");

const Utils = require("../utils");
const REF_ASSETS = require("./../data/vfat_all.json");
const debank_protocol_tags = require("./../data/debank/protocol_list.json");

const global_cache = {};

async function getWalletsCostHistory(wallet_data, job) {
  const global_token_info_debank = await getTokenInfoByDebank(
    wallet_data.wallet
  );
  const global_complex_protocol_debank = await getDeBankComplexProtocol(
    wallet_data.wallet
  );

  // global_token_info_debank = global_token_info_debank.filter(
  //   (x) => !COMPOUNDING_TOKENS[x.id]
  // );

  const wallet_positions = global_token_info_debank.filter(
    (position) => position.is_verified || position.protocol_id != ""
  ); //Skip spam tokens

  const result = [];
  let i = 0;

  let status_job = {
    part: [],
    total: wallet_positions.length,
  };

  const arr = [];

  for (const chain in chainCoins) {
    //if (chain != "eth") continue;

    status_job.part.push({
      current: 0,
      total: 0,
      chain: chainCoins[chain].name_network,
    });

    job.progress(status_job);

    const balances = global_token_info_debank.filter(
      (item) => item.chain == chainCoins[chain].chainId
    );

    if (balances.length == 0) continue;

    const complex = global_complex_protocol_debank.filter(
      (item) => item.chain == chainCoins[chain].chainId
    );

    arr.push(
      getWalletCostBasis(
        {
          chain,
          wallet: wallet_data.wallet,
        },
        balances,
        complex,
        {
          job,
          status: status_job.part[i],
          part: i++,
        }
      )
    );
  }

  try {
    const res = await Promise.all(arr);

    res.forEach((item) => {
      result.push(...item);
    });
  } catch (e) {
    console.log("get wallet cost basis error", e);
    return null;
  }

  //Sort results across chains, largest to smallest value
  result.sort((a, b) => (a.value > b.value ? -1 : 1));
  return result;
}

//Mark "Borrow" transfers
//   For transactions that have:
//   Two transfers in same direction +
//   one of them is a coin being borrowed
//      (on borrow_coin_list in complex_protocol)
function markBorrowTransfers(
  global_complex_protocol_debank,
  global_transfers,
  wallet
) {
  //Find coins being borrowed
  const portfolio_items = global_complex_protocol_debank.flatMap(
    (protocol) => protocol.portfolio_item_list
  );
  const portfolio_items_borrowing = portfolio_items.filter(
    (item) => item.detail.borrow_token_list
  );
  const borrow_tokens = portfolio_items_borrowing.flatMap(
    (item) => item.detail.borrow_token_list
  );
  const borrow_token_ids = borrow_tokens.flatMap((token) => token.id);

  //Loop through transfers in these coins
  const borrow_token_transfers = global_transfers.filter((xfer) =>
    borrow_token_ids.includes(xfer.address)
  );
  for (let i = 0; i < borrow_token_transfers.length; i++) {
    const tx = borrow_token_transfers[i].transaction_hash;
    const transfers_in_tx = global_transfers.filter(
      (xfer) => xfer.transaction_hash == tx
    );
    if (transfers_in_tx.length != 2) continue;
    const to_vault = transfers_in_tx.find(
      (xfer) => xfer.from_address == wallet
    );
    const from_vault = transfers_in_tx.find(
      (xfer) => xfer.to_address == wallet
    );
    if (to_vault && from_vault) continue;

    //If they fit the criteria, mark them as "borrow" transactions
    transfers_in_tx.forEach((xfer) => {
      xfer.type = "borrow";
    });
  }
  return global_transfers;
}

function prepTransfers(
  global_transfers,
  global_tx,
  global_complex_protocol_debank,
  chain,
  wallet
) {
  //Filter out compounding tokens
  // global_transfers = global_transfers.filter(
  //   (x) => !COMPOUNDING_TOKENS[x.address]
  // );

  //Convert strings to numbers
  for (let i = 0; i < global_transfers.length; i++) {
    global_transfers[i].value = BigInt(global_transfers[i].value);
    global_transfers[i].block_number = Number(global_transfers[i].block_number);
    global_transfers[i].to_address =
      global_transfers[i].to_address.toLowerCase();
    global_transfers[i].from_address =
      global_transfers[i].from_address.toLowerCase();
  }

  //Add receipts for one-way vault deposits/withdrawals
  global_transfers = add_vault_transfers(chain, global_transfers, wallet);

  //Copy native inbound transfers
  global_transfers = inbound_native_transfers(global_transfers, chain, wallet);

  //Copy native outbound transfers to ERC20 transfers
  const native_xfers = global_tx.filter((xfer) => xfer.value > 0);
  for (let i = 0; i < native_xfers.length; i++) {
    const tx = native_xfers[i];

    global_transfers.push({
      address: chainCoins[chain].address,
      block_hash: tx.block_hash,
      block_number: tx.block_number,
      block_timestamp: tx.block_timestamp,
      from_address: tx.from_address,
      to_address: tx.to_address,
      transaction_hash: tx.hash,
      value: BigInt(tx.value),
      gas: tx.gas,
      gas_price: tx.gas_price,
    });
  }

  //Mark "Borrow" transfers
  transfers = markBorrowTransfers(
    global_complex_protocol_debank,
    global_transfers,
    wallet
  );

  //Add isReceived
  for (let i = 0; i < global_transfers.length; i++) {
    global_transfers[i]["isReceived"] =
      global_transfers[i].to_address == wallet;
  }

  //Sort: Latest transactions first
  global_transfers = global_transfers.sort(Utils.sortBlockNumber_reverseChrono);

  console.log("global_transfers", global_transfers.length, chain);

  return global_transfers;
}

//Input: wallet vault / defi position from  debank + reference vaults from vfat tools
//Output: deposit address for vault
function getVaultDepositAddress(wallet_vault, ref_assets) {
  let ref_vault;

  const ref_assets_chain_protocol = ref_assets.filter(
    (ref) =>
      ref.chain.toLowerCase() == wallet_vault.chain &&
      ref.protocol.toLowerCase() == wallet_vault.protocol_id
  );

  //Plan A: Match pool id directly
  if (wallet_vault.name == "Governance" || wallet_vault.name == "Locked") {
    ref_vault = ref_assets_chain_protocol.find(
      (ref) => ref.deposit_address == wallet_vault.pool_id.toLowerCase()
    );
  }

  //Plan B: Match on underlying tokens...
  if (!ref_vault) {
    ref_vault = ref_assets_chain_protocol.find(
      (ref) => ref.underlying_tokens_hash == wallet_vault.asset_hash
    );
  }

  //Plan C: Match on deposit tokens...
  if (!ref_vault) {
    ref_vault = ref_assets_chain_protocol.find(
      (ref) => ref.deposit_tokens_hash == wallet_vault.asset_hash
    );
  }

  //if (!ref_vault) return wallet_vault.assets[0];
  if (!ref_vault) return null;
  return ref_vault.deposit_address;
}

//Prep reference vaults
function getGlobalVaults(chain) {
  //TODO: Move this to run when server starts
  let ref_assets = REF_ASSETS.filter(
    (item) => item.chain.toLowerCase() == chain
  );
  for (let i = 0; i < ref_assets.length; i++) {
    // console.log(i);
    // if (i == 86) {
    //   console.log("breakpt");
    // }
    if (ref_assets[i].deposit_tokens) {
      let deposit_tokens_hash = ref_assets[i].deposit_tokens.map((asset) =>
        asset.address.toLowerCase()
      );
      deposit_tokens_hash = deposit_tokens_hash.sort().join("|");
      ref_assets[i]["deposit_tokens_hash"] = deposit_tokens_hash;
    }

    if (ref_assets[i].underlying_tokens) {
      let underlying_tokens_hash = ref_assets[i].underlying_tokens.map(
        (asset) => asset.address?.toLowerCase()
      );
      underlying_tokens_hash = underlying_tokens_hash.sort().join("|");
      ref_assets[i]["underlying_tokens_hash"] = underlying_tokens_hash;
    }
  }
  return ref_assets;
}

//Defi vault positions in wallet
function prepWalletVaults(global_complex_protocol_debank, chain) {
  
  let wallet_vaults = [];
  const complex = global_complex_protocol_debank.filter(
    (item) => item.chain == chain
  );

  //Export complex_protocol into searchable vaults
  for (const complex_protocol_item of complex) {
    const portfolio_item_list = complex_protocol_item.portfolio_item_list;
    if (isDebtProtocol(complex_protocol_item.id)) continue; //AAVE2 has coins for assets + debt
    for (const pool of portfolio_item_list) {
      if (!pool.detail.supply_token_list) continue;
      if (pool.stats.net_usd_value == 0) continue;
      // if (pool.name == "Liquidity Pool") continue; //LPs have coins that will show up in wallet

      const supply_tokens = pool.detail.supply_token_list.map(
        (token) => token.id
      );
      pool.chain = chain;
      pool.protocol_id = complex_protocol_item.id;
      pool.asset_hash = supply_tokens.sort().join("|");
      wallet_vaults.push(pool);
    }
  }

  //Populate deposit addresses into wallet vaults
  const ref_assets = getGlobalVaults(chain); //TODO: Move to when server starts up
  for (let i = 0; i < wallet_vaults.length; i++) {
    const deposit_address = getVaultDepositAddress(wallet_vaults[i],ref_assets);

    // if (!deposit_address) continue;
    wallet_vaults[i]["deposit_address"] = deposit_address;
    wallet_vaults[i]["id"] = deposit_address;
    wallet_vaults[i]["raw_amount"] = BigInt(1e30);
    wallet_vaults[i]["positionType"] = "vault";
    wallet_vaults[i]["value"] = wallet_vaults[i].stats.net_usd_value;
  }
  // wallet_vaults = wallet_vaults.filter((item) => item.deposit_address);
  return wallet_vaults;
}

//Combine defi vault positions with token wallets into 1 list
function prepWallet(
  global_token_info_debank,
  global_complex_protocol_debank,
  chain
) {
  //Tokens in wallet
  let wallet_tokens = global_token_info_debank.filter(
    (position) => (position.is_verified || position.protocol_id != "") && position.is_wallet
  ); //Skip spam tokens
  wallet_tokens = wallet_tokens.map((token) => ({
    ...token,
    positionType: "token",
  }));

  //Defi vaults
  let wallet_vaults = prepWalletVaults(global_complex_protocol_debank, chain);

  // return wallet_vaults;
  if (Utils.isEmpty(wallet_vaults)) {
    return wallet_tokens;
  } else {
    //Vaults first, then tokens
    return [...wallet_vaults, ...wallet_tokens];
  }
}

function add_vault_transfers(chain, global_transfers, wallet) {
  const vaults_in_chain = REF_ASSETS.filter(
    (vault) => vault.chain.toLowerCase() == chain
  );
  //const vaults = vaults_in_chain.map((vault) => getTopVaultAddress(vault));
  const vaults = vaults_in_chain.map((vault) => vault.deposit_address);
  const vaults_uniq = [...new Set(vaults)];

  // const vaults = REF_ASSETS.filter(
  //   (position) => position.positionType == "vault"
  // ).map((position) => position.deposit_address);

  const vault_transfers = global_transfers.filter(
    (xfer) =>
      vaults_uniq.includes(xfer.from_address) ||
      vaults_uniq.includes(xfer.to_address)
  );
  for (let i = 0; i < vault_transfers.length; i++) {
    const tx = vault_transfers[i];
    // if (
    //   tx.transaction_hash ==
    //   "0x645aa0289e7feb83127422b273b420bda6d9269081c0b5b093d442145cfda48f"
    // ) {
    //   console.log("breakpt");
    // }
    //If this transaction has bidirectional transfers (coins to and from vault), do not backfill
    const to_vault = global_transfers.find(
      (xfer) =>
        xfer.transaction_hash == tx.transaction_hash &&
        tx.from_address == wallet
    );
    const from_vault = global_transfers.find(
      (xfer) =>
        xfer.transaction_hash == tx.transaction_hash && tx.to_address == wallet
    );
    if (to_vault && from_vault) continue;

    const vault_address =
      tx.to_address == wallet ? tx.from_address : tx.to_address;
    if (!vaults.includes(vault_address))
      console.log("Error in add_vault_transfers:" + tx);
    global_transfers.push({
      address: vault_address,
      block_hash: tx.block_hash,
      block_number: tx.block_number,
      block_timestamp: tx.block_timestamp,
      from_address: tx.to_address == wallet ? wallet : vault_address,
      to_address: tx.to_address == wallet ? vault_address : wallet,
      isReceived: !tx.isReceived,
      transaction_hash: tx.transaction_hash,
      value: BigInt(1), //placeholder unit
      type: "vault",
    });
  }
  //After adding some transfers, sort reverse chronologically
  global_transfers = global_transfers.sort(Utils.sortBlockNumber_reverseChrono);
  return global_transfers;
}

async function getWalletCostBasis(
  data,
  global_token_info_debank,
  global_complex_protocol_debank,
  { job, status, part }
) {
  console.log("getWalletCostBasis:", data);
  //const chain_blockheight = await getCurrentBlockNumber(data.chain);

  //Get global data
  const result = await Promise.all([
    getTokenBalances(data.chain, data.wallet),
    getTokenTransfers(data.chain, data.wallet),
    getTransactions(data.chain, data.wallet),
  ]);

  let global_balances = result[0];
  let global_transfers = result[1];
  const global_tx = result[2].transactions;
  const global_tx_count = result[2].txCount;
  let lastTxDate = global_transfers[global_transfers.length-1]?.block_timestamp;

  const gb_transfer_tx_ids = [];
  global_transfers.map(transfer => {
    if(!gb_transfer_tx_ids.includes(transfer.transaction_hash)) {
      gb_transfer_tx_ids.push(transfer.transaction_hash);
    }
  });

  const transfer_tx_count = gb_transfer_tx_ids.length;

  global_transfers = prepTransfers(
    global_transfers,
    global_tx,
    global_complex_protocol_debank,
    data.chain,
    data.wallet
  );

  const global_supply_tokens_debank = getSupplyTokens(
    global_complex_protocol_debank
  );

  //If token specified in request, just do that token instead of the whole wallet
  if (data.token) {
    global_balances = global_balances.filter(
      (each) => each.token_address == data.token
    );
  }

  //Set up for positions loop
  let returnData = [];
  const wallet_positions = prepWallet(
    global_token_info_debank,
    global_complex_protocol_debank,
    data.chain
  );

  //Loop through wallet balances, get value + cost basis
  //TODO: Make this loop asynchronous using Promise.all

  for (let i = 0; i < wallet_positions.length; i++) {
    const wallet_position = wallet_positions[i];
    // if (
    //   wallet_position.id ==
    //   "0x619beb58998ed2278e08620f97007e1116d5d25b".toLowerCase()
    // ) {
    //   console.log("getWalletCostBasis:", wallet_position.id);
    // } else {
    //   continue;
    // }
    let tokenHistory = null;
    tokenHistory = await getTokenCostBasis(
      data.chain,
      null, //blockheight
      data.wallet,
      wallet_position,
      wallet_position.id, // address
      BigInt(wallet_position?.raw_amount || 0), //balance
      wallet_position.deposit_address, //for vaults only
      1, // hierarchy_level
      {}, // parent_transaction,
      global_supply_tokens_debank,
      global_transfers,
      global_tx,
      global_token_info_debank
    );

    //Build main table
    let token_result = await makeTokenResult(
      i,
      data.chain,
      wallet_position,
      tokenHistory,
      global_token_info_debank,
      global_complex_protocol_debank,
      global_tx_count,
      lastTxDate,
      transfer_tx_count
    );

    returnData.push(token_result);
    if (status) {
      const progress = await job.progress();
      status.current = i + 1;
      status.total = wallet_positions.length;
      status.ready = true;
      progress[part] = status;
      job.progress(progress);
    }
  }

  //Sort by value, descending
  return returnData;
}

async function makeTokenResult(
  i,
  chain,
  wallet_position,
  tokenHistory,
  global_token_info_debank,
  global_complex_protocol_debank,
  txCount,
  lastTxDate,
  transfer_tx_count
) {
  //console.log("makeTokenResult:", chain);
  // if (chain == "avalanche") {
  //   console.log("breakpt");
  // }
  let token_result = {
    id: "p" + i,
    chain: chain,
    chain_id: 123, //TODO: Chain ID
    chain_logo: debank_chain_details[chain].logo_url,
    type: wallet_position.is_wallet ? "Wallet" : "Yield",
    type_img: wallet_position.is_wallet
      ? "../assets/images/wallet.jpg"
      : "../assets/images/yield.jpg",
    units: wallet_position.amount,
    value:
      wallet_position.value || wallet_position.amount * wallet_position.price,
    cost_basis: tokenHistory.cost_basis,
    history: tokenHistory.history,
    txCount: txCount,
    lastTxDate: lastTxDate,
    transferTxCount: transfer_tx_count
  };

  //Protocol column
  let debank_protocol = null;
  if (wallet_position.protocol_id) {
    debank_protocol = debank_protocol_ref.filter(
      (protocol) => protocol.id == wallet_position.protocol_id
    )[0];
    token_result.protocol_id = wallet_position.protocol_id;
    token_result.protocol = debank_protocol?.name || null;
    token_result.protocol_logo = debank_protocol?.logo_url || null;
    token_result.protocol_url = debank_protocol?.site_url || null;
  }

  //Underlying assets column
  //Plan A: Get it from DeBank
  if (wallet_position.detail?.supply_token_list || false) {
    token_result.assets = wallet_position.detail.supply_token_list.map(
      (asset) => ({
        id: asset.id,
        ticker: asset.optimized_symbol,
        logo: asset.logo_url,
      })
    );
    //Plan B: Wallet coin is its own asset
  } else if (wallet_position.is_wallet) {
    token_result.assets = [
      {
        id: wallet_position.id,
        ticker: wallet_position.symbol,
        logo: wallet_position.logo_url || debank_protocol?.logo_url || null,
      },
    ];
    //Plan C: Guess underlying asset from history
  } else {
    //TODO: pass in JSON_CURVE and find the assets from 3CRV to underlying.
    token_result.assets = await getAssets(
      chain,
      tokenHistory.history,
      global_token_info_debank
    ); //Copy liquid assets from tree here
  }

  //If value is blank, fill it in from debank complex protocol api
  if (
    token_result.value == 0 &&
    wallet_position.protocol_id &&
    debank_protocol
  ) {
    if (
      token_result.cost_basis < 0 &&
      isDebtProtocol(wallet_position.protocol_id)
    ) {
      token_result.value = await getDebtValue(
        wallet_position,
        token_result.assets,
        global_complex_protocol_debank,
        global_token_info_debank
      );
    } else {
      token_result.value = getDebankValue(
        wallet_position.id,
        debank_protocol,
        token_result.assets,
        global_complex_protocol_debank
      );
    }
  }

  return token_result;
}

function isDebtProtocol(protocol_id) {
  const protocol = debank_protocol_tags.data.find(
    (debank_protocol) => debank_protocol.id == protocol_id
  );
  if (!protocol) return false;
  const isDebt = protocol.tag_ids.includes("debt");
  return isDebt;
}

async function getDebtValue(
  wallet_position,
  assets,
  global_complex_protocol_debank,
  global_token_info_debank
) {
  let borrow_token; //so that the variable works outside the try{} block
  const borrowed_asset_id = assets[0].id; //What if >1 asset is borrowed?
  const lending_protocol = global_complex_protocol_debank.filter(
    (protocol) => protocol.id == wallet_position.protocol_id
  );

  //Find coins being borrowed in complex protocol
  try {
    const portfolio_items = lending_protocol.flatMap(
      (protocol) => protocol.portfolio_item_list
    );
    const portfolio_items_borrowing = portfolio_items.filter(
      (item) => item.detail.borrow_token_list
    );
    const borrow_tokens = portfolio_items_borrowing.flatMap(
      (item) => item.detail.borrow_token_list
    );
    borrow_token = borrow_tokens.find((token) => token.id == borrowed_asset_id);
    const amount = borrow_token.amount;
  } catch (error) {
    console.log("getDebtValue: No borrowed tokens found");
    return 0;
  }

  const price = await getTokenPrice(
    wallet_position.chain,
    borrow_token.id,
    null, // _toBlock,
    global_token_info_debank
  );
  const debt_value = borrow_token.amount * price * -1; //negative cost = credit to account
  return debt_value;
}

async function getTokenCostBasis(
  chain,
  blockheight,
  wallet,
  wallet_position,
  token,
  balance,
  deposit_address, //for vaults only
  hierarchy_level,
  parent_transaction,
  global_supply_tokens_debank,
  global_transfers,
  global_tx,
  global_token_info_debank,
  reverse = true
) {
  let token_cost = 0,
    current_balance = BigInt(balance),
    token_info = null,
    price = null,
    newHistory = [];

  //Get token price
  if (!deposit_address && token) {
    token_info = await get_debank_token(chain, token, global_token_info_debank);
    if(token_info) {
      token_info.decimals = token_info.decimals || 18;
      if (blockheight) {
        //historical price
        price = await getTokenPrice(
          chain,
          token,
          blockheight,
          global_token_info_debank
        );
      } else {
        //current price
        price = token_info.price;
      }
    }
  }

  //Is this one of the underlying tokens?
  const is_supply_token = global_supply_tokens_debank.includes(token);
  const units = Number(balance) / 10 ** (token_info?.decimals || 18);

  //Liquid tokens
  if (
    (Math.abs(units * price) < 1 && price > 0) || //small position
    (hierarchy_level == 1 && token_info && token_info.is_core) ||
    (hierarchy_level > 1 &&
      price &&
      (is_supply_token ||
        token_info.debank_not_found ||
        parent_transaction.type == "borrow" ||
        parent_transaction.type == "vault")) ||
    (hierarchy_level > 2 && price)
  ) {
    token_cost = units * price;
    if (!Utils.isEmpty(parent_transaction)) {
      //hierarchy_level>1
      newHistory.push({
        units,
        transaction_id: parent_transaction.transaction_hash,
        transaction_url: `${chainCoins[chain].explorer}/${parent_transaction.transaction_hash}`,
        datetime: Utils.convertDateTime(parent_transaction.block_timestamp),
        token_id: token,
        token_name: token_info?.name || debank_protocol_ref.filter((protocol) => protocol.id == wallet_position.protocol_id)[0]?.name+" vault receipt",
        token_symbol: token_info?.symbol || "<Unknown symbol>",
        token_img: token_info?.logo_url || null,
        fee_native_coin: chainCoins[chain].native_coin,
        cost_basis: token_cost,
        hierarchy_level,
        valued_directly: true,
      });
    }
    return { cost_basis: token_cost, history: newHistory };
  }

  // Non-wallet tokens

  // retrieve list of token transactions to/from wallet, prior to block
  let token_transactions = global_transfers.filter(
    (xfer) =>
      (Utils.isEmpty(parent_transaction) ? true : xfer.isReceived == reverse) && //In L2+, look for only buys or only sells
      (deposit_address //this is a vault
        ? xfer.type == "vault" &&
          (xfer.to_address == deposit_address ||
            xfer.from_address == deposit_address)
        : xfer.address == token) &&
      xfer.used == undefined &&
      xfer.value > 0 &&
      xfer.address != parent_transaction.address &&
      (reverse
        ? Number(xfer.block_number) <= Number(blockheight || 1e20)
        : Number(xfer.block_number) >= Number(blockheight || 1e20))
  );

  if (!reverse) {token_transactions = token_transactions.sort(Utils.sortBlockNumber_Chrono);}

  // For each transaction
  for (let i = 0; i < token_transactions.length; i++) {
    const transaction = token_transactions[i];
    transaction.used = true;
    let transaction_cost = 0, used_pct = 1;

    const transaction_detail = global_tx.filter((tx) => tx.hash === transaction.transaction_hash)[0] || {};

    //calculate the balance of token in wallet, just before transaction.
    const isReceived = transaction.isReceived;
    const units_received = transaction.value * (isReceived ? 1n : -1n);
    if (isReceived && current_balance < transaction.value) {
      used_pct = Number(current_balance) / Number(transaction.value);
      current_balance = 0;
    } else {
      used_pct = 1;
      current_balance = current_balance - units_received;
    }

    // calculate the cost basis of current transaction, starting w/offseting coins
    let offsetting_coins = global_transfers.filter(
      (xfer) =>
        xfer.transaction_hash == transaction.transaction_hash &&
        xfer.used == undefined &&
        (transaction.type == "borrow" ? true : xfer.isReceived != isReceived)
      //For normal transactions, offsetting transfers is in opposite direction (!isReceive)
      //For borrow transactions, it's in the same direction
    );

    //If coin was sent, sort chronological to look for future dispositions
    if (!isReceived) {
      offsetting_coins = offsetting_coins.sort(Utils.sortBlockNumber_Chrono);
    }

    let childHistory = [];

    for (let i = 0; i < offsetting_coins.length; i++) {
      const offsetting_coin = offsetting_coins[i];
      offsetting_coin.used = true;
      let offsetting_coin_units = offsetting_coin.value * (isReceived ? 1n : -1n) * (transaction.type == "borrow" ? -1n : 1n);
      //  For borrow transactions: debt and borrowed token move in same direction
      if (used_pct < 1) {
        offsetting_coin_units = Number(offsetting_coin_units) * used_pct;
        offsetting_coin_units = BigInt(Math.round(offsetting_coin_units));
      }

      const CostBasisResult = await getTokenCostBasis(
        chain,
        offsetting_coin.block_number,
        wallet,
        wallet_position,
        offsetting_coin.address,
        offsetting_coin_units, //balances
        null, //deposit_address, for vaults only
        hierarchy_level + 1,
        transaction, // parent transaction (transfer)
        global_supply_tokens_debank,
        global_transfers,
        global_tx,
        global_token_info_debank,
        isReceived
      );
      transaction_cost = transaction_cost + CostBasisResult.cost_basis;

      childHistory = childHistory.concat(CostBasisResult.history);
    }

    token_cost = token_cost + transaction_cost;
    const native_price = await getTokenPrice(
      chain,
      chainCoins[chain].address,
      blockheight,
      global_token_info_debank
    );

    const native_token_info = await get_debank_token(
      chain,
      chainCoins[chain].address,
      global_token_info_debank
    );
    const fee_native_units = (transaction_detail.gas * transaction_detail.gas_price) / 10 ** (native_token_info?.decimals || 18);
    let units = Number(units_received) / 10 ** (token_info?.decimals || 18);
    if (used_pct < 1) {
      units = Number(units) * used_pct;
      units = Math.round(units);
    }

    newHistory.push({
      units: units,
      transaction_id: transaction.transaction_hash,
      transaction_url: `${chainCoins[chain].explorer}/${transaction.transaction_hash}`,
      datetime: Utils.convertDateTime(transaction.block_timestamp),
      token_id: token,
      token_name: token_info?.name || debank_protocol_ref.filter((protocol) => protocol.id == wallet_position.protocol_id)[0]?.name+" vault receipt",
      token_symbol: token_info?.symbol,
      token_img: token_info?.logo_url || debank_protocol_ref.filter( (p) => p.id == token_info?.protocol_id || 0 )[0]?.logo_url || null,
      fee_native_coin: chainCoins[chain].native_coin,
      fee_native_units: fee_native_units,
      fee_usd: fee_native_units * native_price || 0,
      cost_basis: transaction_cost,
      used_pct: used_pct,
      hierarchy_level: hierarchy_level,
      valued_directly: false,
      child: childHistory,
    });

    if (current_balance <= 0) break;
  } //end token transaction loop

  return { cost_basis: token_cost, history: newHistory };
}

//Log eth withdrawals from AAVE: aWETH outbound, ETH inbound
function inbound_native_transfers(transfers, chain, wallet) {
  if (chain != "eth") return transfers;
  const AWETH_ADDRESS = "0x030ba81f1c18d280636f32af80b9aad02cf0854e";
  // const WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
  // const AAWE_WETH_GETEWAY = "0xcc9a0b7c43dc2a5f023bb9b738e45b0ef6b06e04";

  const aave_eth_withdrawals = transfers.filter(
    (xfer) => xfer.address == AWETH_ADDRESS && xfer.from_address == wallet
  );
  for (let i = 0; i < aave_eth_withdrawals.length; i++) {
    const xfer = aave_eth_withdrawals[i];
    transfers.push({
      address: chainCoins[chain].address, //WETH transfer
      block_hash: xfer.block_hash,
      block_number: xfer.block_number,
      block_timestamp: xfer.block_timestamp,
      from_address: xfer.to_address, //from AAVE ETH Router
      to_address: wallet, //to wallet
      transaction_hash: xfer.transaction_hash,
      value: xfer.value,
    });
  }

  return transfers;
}

module.exports = {
  getWalletsCostHistory,
};
