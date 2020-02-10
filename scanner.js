const async = require('async')
const util = require('util')
const events = require('events')
const _ = require('lodash')
const DATransaction = require('digiasset-transaction')
const bitcoin = require('bitcoin-async')
const get_assets_outputs = require('digiasset-get-assets-outputs')
const squel = require('squel')
squel.cls.DefaultQueryBuilderOptions.autoQuoteFieldNames = true
squel.cls.DefaultQueryBuilderOptions.nameQuoteCharacter = '"'
squel.cls.DefaultQueryBuilderOptions.separator = '\n'
const sql_builder = require('nodejs-sql')(squel)

let properties
let bitcoin_rpc
let debug

function Scanner (settings, db) {

  debug = settings.debug
  this.to_revert = []
  this.priority_parse_list = []
  properties = settings.properties
  bitcoin_rpc = new bitcoin.Client(settings.rpc_settings)

  this.sequelize = db.sequelize
  this.Sequelize = db.Sequelize
  this.Outputs = db.outputs
  this.Inputs = db.inputs
  this.Blocks = db.blocks
  this.Blocks.properties = properties
  this.Transactions = db.transactions
  this.Transactions.properties = properties
  this.AddressesOutputs = db.addressesoutputs
  this.AddressesTransactions = db.addressestransactions
  this.AssetsOutputs = db.assetsoutputs
  this.AssetsTransactions = db.assetstransactions
  this.AssetsAddresses = db.assetsaddresses
  this.Assets = db.assets

  if (process.env.ROLE !== properties.roles.API) {
    this.on('newblock', (newblock) => {
      process.send({to: properties.roles.API, newblock: newblock})
    })
    this.on('newtransaction', (newtransaction) => {
      process.send({to: properties.roles.API, newtransaction: newtransaction})
    })
    this.on('newdatransaction', (newdatransaction) => {
      process.send({to: properties.roles.API, newdatransaction: newdatransaction})
    })
    this.on('revertedblock', (revertedblock) => {
      process.send({to: properties.roles.API, revertedblock: revertedblock})
    })
    this.on('revertedtransaction', (revertedtransaction) => {
      process.send({to: properties.roles.API, revertedtransaction: revertedtransaction})
    })
    this.on('reverteddatransaction', (reverteddatransaction) => {
      process.send({to: properties.roles.API, reverteddactransaction: reverteddatransaction})
    })
    this.on('mempool', () => {
      process.send({to: properties.roles.API, mempool: true})
    })
  }

  this.mempool_cargo = async.cargo((tasks, callback) => {
    console.log('async.cargo() - parse_mempool_cargo')
    this.parse_mempool_cargo(tasks, callback)
  }, 100)
}

util.inherits(Scanner, events.EventEmitter)

Scanner.prototype.scan_blocks = function (err) {
  if (err) {
    console.error('scan_blocks: err = ', err)
    return this.scan_blocks()
  }
  let job
  let next_block
  let last_hash

  async.waterfall([
    (cb) => {
      console.log('scan_blocks #1')
      this.get_next_new_block(cb)
    }, (l_next_block, l_last_hash, cb) => {
      console.log('scan_blocks #2, l_next_block = ', l_next_block, ' l_last_hash = ', l_last_hash)
      last_hash = l_last_hash
      next_block = l_next_block || 0
      this.get_raw_block(next_block, cb)
    }, (raw_block_data, cb) => {
      console.log('scan_blocks #3, raw_block_data.height = ', raw_block_data.height)
      if (!cb) {
        cb = raw_block_data
        raw_block_data = null
      }
      if (!raw_block_data || (raw_block_data.height === next_block - 1 && raw_block_data.hash === last_hash)) {
        setTimeout(() => {
          if (debug) {
            job = 'mempool_scan'
            console.time(job)
          }
          this.parse_new_mempool(cb)
        }, 500)
      } else if (!raw_block_data.previousblockhash || raw_block_data.previousblockhash === last_hash) {
        if (debug) {
          job = 'parse_new_block'
          console.time(job)
        }
        this.parse_new_block(raw_block_data, cb)
      } else {
        if (debug) {
          job = 'reverting_block'
          console.time(job)
        }
        this.revert_block(next_block - 1, cb)
      }
    }
  ], (err) => {
    if (debug && job) console.timeEnd(job)
    if (err) {
      this.to_revert = []
      this.mempool_txs = null
      console.log('scan_blocks - err = ', err)
    }
    console.log(' --- ended scan_blocks ----')
    this.scan_blocks(err)
  })
}

Scanner.prototype.revert_block = function (block_height, callback) {
  console.log('Reverting block: ' + block_height)
  const find_block_query = '' +
    'SELECT\n' +
    '  hash,\n' +
    '  previousblockhash,\n' +
    '  to_json(array(\n' +
    '    SELECT\n' +
    '      transactions.txid\n' +
    '    FROM\n' +
    '      (SELECT\n' +
    '          txid\n' +
    '        FROM\n' +
    '          transactions\n' +
    '        WHERE\n' +
    '          transactions.blockheight = blocks.height\n' +
    '        ORDER BY\n' +
    '          transactions.index_in_block) AS transactions)) AS tx\n' +
    'FROM\n' +
    '  blocks\n' +
    'WHERE\n' +
    '  blocks.height = :height'

    this.sequelize.query(find_block_query, {type: this.sequelize.QueryTypes.SELECT, replacements: {height: block_height}})
    .then((block_data) => {
      if (!block_data || !block_data.length) return callback('no block for height ' + block_height)
      block_data = block_data[0]
      if (!block_data.tx) return callback('no transactions in block for height ' + block_height)
      const block_id = {
        height: block_height,
        hash: block_data.hash
      }

      // logger.debug('reverting '+block_data.tx.length+' txs.')
      const txids = []
      const colored_txids = []
      const sql_query = []
      async.mapSeries(block_data.tx.reverse(), (txid, cb) => {
        txids.push(txid)
        this.revert_tx(txid, sql_query, (err, colored, revert_flags_txids) => {
          if (err) return cb(err)
          if (colored) {
            colored_txids.push(txid)
          }
          cb(null, revert_flags_txids)
        })
      }, (err, revert_flags_txids) => {
        if (err) return callback(err)
        revert_transactions_flags(revert_flags_txids, sql_query)
        sql_query = sql_query.join(';\n')
        this.sequelize.query(sql_query)
          .then(() => {
            txids.forEach((txid) => {
              this.emit('revertedtransaction', {txid: txid})
            })
            colored_txids.forEach((txid) => {
              this.emit('reverteddatransaction', {txid: txid})
            })
            this.fix_mempool((err) => {
              if (err) return callback(err)
              const delete_blocks_query = squel.delete()
                .from('blocks')
                .where('height = ?', block_height)
                .toString()
                this.sequelize.query(delete_blocks_query)
                .then(() => {
                  this.emit('revertedblock', block_id)
                  set_last_hash(block_data.previousblockhash)
                  set_last_block(block_data.height - 1)
                  set_next_hash(null)
                  callback()
                }).catch(callback)
            })
          }).catch(callback)
      })
    }).catch(callback)
}

const revert_transactions_flags = function (txids, sql_query) {
  sql_query = sql_query || []
  txids = [].concat.apply([], txids)
  txids = _(txids).uniq().filter((txid) => { return txid }).value()
  console.log('revert flags txids:', txids)
  if (!txids.length) {
    return
  }
  // put these 2 updates first. Otherwise delete queries will make it unrecoverable.
  sql_query.unshift(squel.update()
    .table('transactions')
    .set('iosparsed', false)
    .set('daparsed', false)
    .where('txid IN ?', txids)
    .toString())
  sql_query.unshift(squel.update()
    .table('inputs')
    .set('output_id', null)
    .where('input_txid IN ?', txids)
    .toString())
}

Scanner.prototype.fix_mempool = function (callback) {
  const fix_mempool_query = squel.update()
    .table('transactions')
    .set('iosparsed', false)
    .set('daparsed', false)
    .where('blockheight = -1')
    .toString()
  this.sequelize.query(fix_mempool_query).then(() => { callback() }).catch(callback)
}

Scanner.prototype.revert_tx = function (txid, sql_query, callback) {
  // console.log('reverting tx ' + txid)
  const find_transaction_query = '' +
    'SELECT\n' +
    '  transactions.colored,\n' +
    '  to_json(array(\n' +
    '    SELECT\n' +
    '      vin\n' +
    '    FROM\n' +
    '      (SELECT\n' +
    '        inputs.input_index,\n' +
    '        inputs.txid,\n' +
    '        inputs.vout\n' +
    '      FROM\n' +
    '        inputs\n' +
    '      WHERE\n' +
    '        inputs.input_txid = transactions.txid\n' +
    '      ORDER BY input_index) AS vin)) AS vin,\n' +
    '  to_json(array(\n' +
    '    SELECT\n' +
    '      vout\n' +
    '    FROM\n' +
    '      (SELECT\n' +
    '        outputs.id,\n' +
    '        outputs."usedTxid"\n' +
    '      FROM\n' +
    '        outputs\n' +
    '      WHERE outputs.txid = transactions.txid\n' +
    '      ORDER BY n) AS vout)) AS vout\n' +
    'FROM\n' +
    '  transactions\n' +
    'WHERE\n' +
    '  txid = :txid'
  this.sequelize.query(find_transaction_query, {replacements: {txid: txid}, type: this.sequelize.QueryTypes.SELECT})
    .then((transactions) => {
      if (!transactions || !transactions.length) {
        console.log('revert_tx: txid ' + txid + ', transactions && transactions.length = ' + (transactions && transactions.length))
        return callback()
      }
      const next_txids = []
      const transaction = transactions[0]
      this.revert_vin(txid, transaction.vin, sql_query)
      this.revert_vout(transaction.vout, sql_query)
      sql_query.push(squel.delete()
        .from('addressestransactions')
        .where('txid = ?', txid)
        .toString())
      sql_query.push(squel.delete()
        .from('assetstransactions')
        .where('txid = ?', txid)
        .toString())
      sql_query.push(squel.delete()
        .from('transactions')
        .where('txid = ?', txid)
        .toString())

      transaction.vout.forEach((output) => {
        if (output.usedTxid && next_txids.indexOf(output.usedTxid) === -1) {
          next_txids.push(output.usedTxid)
        }
      })
      callback(null, transaction.colored, next_txids)
    })
    .catch(callback)
}

Scanner.prototype.revert_vin = function (txid, vin, sql_query) {
  if (!vin || !vin.length || vin[0].coinbase) return
  vin.forEach((input) => {
    if (input.txid && input.vout) {
      sql_query.push(squel.update()
        .table('outputs')
        .set('used', false)
        .set('usedTxid', null)
        .set('usedBlockheight', null)
        .where('txid = ? AND n = ? AND "usedTxid" = ?', input.txid, input.vout, txid)
        .toString())
    }
    sql_query.push(squel.delete()
      .from('inputs')
      .where('input_txid = ? AND input_index = ?', txid, input.input_index)
      .toString())
  })
}

Scanner.prototype.revert_vout = function (vout, sql_query) {
  if (!vout || !vout.length) return
  vout.forEach((output) => {
    sql_query.push(squel.delete()
      .from('addressesoutputs')
      .where('output_id = ?', output.id)
      .toString())
    sql_query.push(squel.delete()
      .from('assetsoutputs')
      .where('output_id = ?', output.id)
      .toString())
    sql_query.push(squel.delete()
      .from('outputs')
      .where('id = ?', output.id)
      .toString())
  })
}

Scanner.prototype.get_next_block_to_fix = function (limit, callback) {
  const conditions = {
    txsinserted: true, txsparsed: false
  }
  this.Blocks.findAll({
    where: conditions,
    attributes: ['height', 'hash'],
    limit: limit,
    order: [['height', 'ASC']],
    raw: true
  }).then((blocks) => {
    console.log('get_next_block_to_fix - found ' + blocks.length + ' blocks')
    callback(null, blocks)
  }).catch((e) => {
    console.log('get_next_block_to_fix - e = ', e)
    callback(e)
  })
}

Scanner.prototype.get_next_new_block = function (callback) {
  console.log('get_next_new_block')
  if (properties.last_block && properties.last_hash) {
    return callback(null, properties.last_block + 1, properties.last_hash)
  }
  this.Blocks.findOne({
    where: {txsinserted: true},
    attributes: ['height', 'hash'],
    order: [['height', 'DESC']],
    raw: true
  }).then((block) => {
    if (block) {
      set_last_block(block.height)
      set_last_hash(block.hash)
      callback(null, block.height + 1, block.hash)
    } else {
      callback(null, 0, null)
    }
  }).catch(callback)
}

Scanner.prototype.get_raw_block = function (block_height, callback) {
  console.log('get_raw_block(' + block_height + ')')
  bitcoin_rpc.cmd('getblockhash', [block_height], (err, hash) => {
    if (err) {
      if ('code' in err && err.code === -8) {
        // logger.debug('CODE -8!!!')
        bitcoin_rpc.cmd('getblockcount', [], (err, block_count) => {
          if (err) return callback(err)
          if (block_count < block_height) {
            return this.get_raw_block(block_count, callback)
          } else {
            return callback()
          }
        })
      } else {
        callback(err)
      }
    } else if (hash) {
      bitcoin_rpc.cmd('getblock', [hash], callback)
    } else {
      bitcoin_rpc.cmd('getblockcount', [], (err, block_count) => {
        if (err) return callback(err)
        if (block_count < block_height) {
          return this.get_raw_block(block_count, callback)
        } else {
          return callback()
        }
      })
    }
  })
}

Scanner.prototype.parse_new_block = function (raw_block_data, callback) {
  raw_block_data.time = raw_block_data.time * 1000
  raw_block_data.txsparsed = false
  raw_block_data.txsinserted = false
  raw_block_data.daparsed = false
  raw_block_data.reward = calc_block_reward(raw_block_data.height)
  raw_block_data.totalsent = 0
  raw_block_data.fees = 0
  raw_block_data.txlength = raw_block_data.tx.length
  console.log('parsing new block ' + raw_block_data.height)

  this.to_revert = []
  if (this.mempool_txs) {
    _.pullAllWith(this.mempool_txs, raw_block_data.tx, (tx, txid) => {
      return tx.txid === txid
    })
  }
  const command_arr = raw_block_data.tx.map((txid) => { return {method: 'getrawtransaction', params: [txid, 1]} })

  let index_in_block = 0
  bitcoin_rpc.cmd(command_arr, (raw_transaction_data, cb) => {
    const sql_query = []
    raw_transaction_data = to_discrete(raw_transaction_data)
    raw_transaction_data.index_in_block = index_in_block
    index_in_block++
    const out = this.parse_new_transaction(raw_transaction_data, raw_block_data.height, sql_query)
    if (out) {
      raw_block_data.totalsent += out
      if (is_coinbase(raw_transaction_data)) {
        raw_block_data.fees = out || raw_block_data.reward
        raw_block_data.fees -= raw_block_data.reward
      }
    }
    if (!sql_query.length) return cb()
    sql_query = sql_query.join(';\n')
    self.sequelize.transaction((sql_transaction) => {
      return this.sequelize.query(sql_query, {transaction: sql_transaction})
        .then(() => { cb() })
        .catch(cb)
    })
  }, (err) => {
    let sql_query = ''
    if (err) {
      if ('code' in err && err.code === -5) {
        console.error('Can\'t find tx.')
      } else {
        console.error('parse_new_block_err: ', err)
        return callback(err)
      }
    }
    if (debug) console.time('vout_parse_bulks')
    raw_block_data.txsinserted = true

    sql_query += squel.insert()
      .into('blocks')
      .setFields(to_sql_fields(raw_block_data, {exclude: ['tx', 'confirmations']}))
      .toString() + ' ON CONFLICT (hash) DO UPDATE SET txsinserted = TRUE;'

    if (!properties.next_hash && raw_block_data.previousblockhash) {
      sql_query += squel.update()
        .table('blocks')
        .set('nextblockhash', raw_block_data.hash)
        .where('hash = ?', raw_block_data.previousblockhash)
        .toString()
    }

    const close_block = function () {
      set_last_hash(raw_block_data.hash)
      set_last_block(raw_block_data.height)
      set_next_hash(raw_block_data.nextblockhash)
      callback()
    }

    this.sequelize.transaction((sql_transaction) => {
      return this.sequelize.query(sql_query, {transaction: sql_transaction})
        .then(close_block)
        .catch(callback)
    })
  })
}

const get_opreturn_data = function (asm) {
  return asm.substring('OP_RETURN '.length)
}

const check_version = function (hex) {
  const version = hex.toString('hex').substring(0, 4)
  return (version.toLowerCase() === '4441')
}

Scanner.prototype.parse_new_transaction = function (raw_transaction_data, block_height, sql_query) {
  if (raw_transaction_data.time) {
    raw_transaction_data.time = raw_transaction_data.time * 1000
  }
  if (raw_transaction_data.blocktime) {
    raw_transaction_data.blocktime = raw_transaction_data.blocktime * 1000
  } else {
    raw_transaction_data.blocktime = Date.now()
    if (block_height !== -1) console.log('yoorika!')
  }
  raw_transaction_data.blockheight = block_height

  this.parse_vin(raw_transaction_data, block_height, sql_query)
  const out = this.parse_vout(raw_transaction_data, block_height, sql_query)

  raw_transaction_data.iosparsed = false
  raw_transaction_data.daparsed = false
  const update = {
    blocktime: raw_transaction_data.blocktime,
    blockheight: raw_transaction_data.blockheight,
    blockhash: raw_transaction_data.blockhash,
    time: raw_transaction_data.time,
    index_in_block: raw_transaction_data.index_in_block
  }

  // put this query first because of outputs and inputs foreign key constraints, validate transaction in DB
  sql_query.unshift(squel.insert()
    .into('transactions')
    .setFields(to_sql_fields(raw_transaction_data, {exclude: ['vin', 'vout', 'confirmations']}))
    .toString() + ' ON CONFLICT (txid) DO UPDATE SET ' + sql_builder.to_update_string(update))

  return out
}

// todo actual block reward
var calc_block_reward = function (block_height) {
  var reward = 50 * 100000000
  var divistions = Math.floor(block_height / 210000)
  while (divistions) {
    reward /= 2
    divistions--
  }
  return reward
}

const get_block_height = function (blockhash, callback) {
  bitcoin_rpc.cmd('getblock', [blockhash], (err, block) => {
    if (err) return callback(err)
    callback(null, block.height)
  })
}

Scanner.prototype.parse_vin = function (raw_transaction_data, block_height, sql_query) {
  if (!raw_transaction_data.vin) return
  raw_transaction_data.vin.forEach((vin, index) => {
    vin.input_txid = raw_transaction_data.txid
    vin.input_index = index
    sql_query.push(squel.insert()
      .into('inputs')
      .setFields(to_sql_fields(vin))
      .toString() + ' ON CONFLICT (input_txid, input_index) DO NOTHING')
  })
}

Scanner.prototype.parse_vout = function (raw_transaction_data, block_height, sql_query) {
  let out = 0
  const addresses = []
  if (!raw_transaction_data.vout) return 0
  raw_transaction_data.dadata = raw_transaction_data.dadata || []
  raw_transaction_data.vout.forEach((vout) => {
    if (vout.scriptPubKey.hex.length > 2000) {
      vout.scriptPubKey.hex = null
      vout.scriptPubKey.asm = 'TOBIG'
    } else if (vout.scriptPubKey && vout.scriptPubKey.type === 'nulldata') {
      const hex = get_opreturn_data(vout.scriptPubKey.asm)
      if (check_version(hex)) {
        try {
          const da = DATransaction.fromHex(hex).toJson()
        } catch (e) {
          console.log('Invalid DA transaction.')
        }
        if (da) {
          raw_transaction_data.dadata.push(da)
          raw_transaction_data.colored = true
        }
      }
    }

    out += vout.value

    const new_utxo = {
      txid: raw_transaction_data.txid,
      n: vout.n,
      value: vout.value
    }
    if (vout.scriptPubKey) {
      new_utxo.scriptPubKey = vout.scriptPubKey
    }

    sql_query.push(squel.insert()
      .into('outputs')
      .setFields(to_sql_fields(new_utxo))
      .toString() + ' ON CONFLICT (txid, n) DO NOTHING')

    if (vout.scriptPubKey && vout.scriptPubKey.addresses) {
      vout.scriptPubKey.addresses.forEach((address) => {
        addresses.push(address)
        sql_query.push(squel.insert()
          .into('addressesoutputs')
          .set('address', address)
          .set('output_id',
            squel.select().field('id').from('outputs').where('txid = ? AND n = ?', new_utxo.txid, new_utxo.n))
          .toString() + ' ON CONFLICT (address, output_id) DO NOTHING')
      })
    }
  })

  addresses = _.uniq(addresses)
  addresses.forEach((address) => {
    sql_query.push(squel.insert()
      .into('addressestransactions')
      .set('address', address)
      .set('txid', raw_transaction_data.txid)
      .toString() + ' ON CONFLICT (address, txid) DO NOTHING')
  })

  return out
}

Scanner.prototype.scan_mempol_only = function (err) {
  if (err) {
    console.error(err)
    return this.scan_mempol_only()
  }
  this.parse_new_mempool((err) => {
    setTimeout(() => {
      this.scan_mempol_only(err)
    }, 500)
  })
}

Scanner.prototype.fix_blocks = function (err, callback) {
  if (err) {
    console.error('fix_blocks: err = ', err)
    return this.fix_blocks(null, callback)
  }
  const emits = [];
  const cback = (err) => {
    this.fix_blocks(err)
  }
  callback = callback || cback;
  this.get_next_block_to_fix(50, (err, raw_block_datas) => {
    if (err) return callback(err)
    console.log('fix_blocks - get_next_block_to_fix: ' + raw_block_datas.length + ' blocks')
    if (!raw_block_datas || !raw_block_datas.length) {
      return setTimeout(() => {
        callback()
      }, 500)
    }
    const first_block = raw_block_datas[0].height
    const num_of_blocks = raw_block_datas.length
    const last_block = raw_block_datas[num_of_blocks - 1].height

    const close_blocks = (err, empty) => {
      console.log('fix_blocks - close_blocks: err = ', err, ', empty = ', empty)
      if (err) return callback(err)
      emits.forEach((emit) => {
        this.emit(emit[0], emit[1])
      })
      if (!empty) return callback()
      const blocks_heights = _.map(raw_block_datas, 'height')
      if (!blocks_heights.length) {
        return setTimeout(() => {
          callback()
        }, 500)
      }
      const update = {
        txsparsed: true,
        daparsed: false
      }
      const conditions = {
        height: {$between: [first_block, last_block]}
      }
      this.Blocks.update(
        update,
        {
          where: conditions
        }
      ).then((res) => {
        console.log('fix_blocks - close_blocks - success')
        callback()
      }).catch((e) => {
        console.log('fix_blocks - close_blocks - e = ', e)
        callback(e)
      })
    }

    this.get_need_to_fix_transactions_by_blocks(first_block, last_block, (err, transactions_datas) => {
      if (err) return callback(err)
      console.log('Fixing blocks ' + first_block + '-' + last_block + ' (' + transactions_datas.length + ' txs).')
      if (!transactions_datas) return callback('can\'t get transactions from db')
      if (!transactions_datas.length) {
        return close_blocks(null, true)
      }
      console.time('fix_transactions')
      console.time('fix_transactions - each')
      const bulk_outputs_ids = []
      const bulk_inputs = []
      async.each(transactions_datas, (transaction_data, cb) => {
        this.fix_vin(transaction_data, transaction_data.blockheight, bulk_outputs_ids, bulk_inputs, (err) => {
          if (err) return cb(err)
          // console.time('fix_transactions - fix bulk')
          if (!transaction_data.colored && transaction_data.iosparsed) {
            emits.push(['newtransaction', transaction_data])
          }
          cb()
        })
      }, (err) => {
        if (err) return callback(err)
        console.timeEnd('fix_transactions - each')
        console.log('fix_transactions - outputs.length = ', bulk_outputs_ids.length)
        console.log('fix_transactions - inputs.length = ', bulk_inputs.length)
        console.log('fix_transactions - transactions.length = ', transactions_datas.length)
        const queries = get_fix_transactions_update_query(bulk_outputs_ids, bulk_inputs, transactions_datas)
        const outputs_query = queries.outputs_query
        const inputs_query = queries.inputs_query
        const transactions_query = queries.transactions_query

        console.time('fix_transactions - parallel')
        async.parallel([
          (cb) => {
            if (!bulk_outputs_ids.length) return cb()
            console.time('fix_transactions - outputs')
            this.sequelize.query(outputs_query).then(() => {
              console.timeEnd('fix_transactions - outputs')
              cb()
            }).catch(cb)
          }, (cb) => {
            if (!bulk_inputs.length) return cb()
            console.time('fix_transactions - inputs')
            this.sequelize.query(inputs_query).then(() => {
              console.timeEnd('fix_transactions - inputs')
              cb()
            }).catch(cb)
          }
        ], (err) => {
          if (err) {
            console.log('fix_transactions - err = ', JSON.stringify(err))
            return callback(err)
          }
          console.time('fix_transactions - transactions')
          this.sequelize.query(transactions_query).then(() => {
            console.timeEnd('fix_transactions - transactions')
            console.timeEnd('fix_transactions - parallel')
            console.timeEnd('fix_transactions')
            close_blocks()
          }).catch(callback)
        })
      })
    })
  })
}

const get_fix_transactions_update_query = function (bulk_outputs_ids, bulk_inputs, transactions) {
  const ans = {}

  if (bulk_outputs_ids.length) {
    const outputs_conditions = 'outputs.id IN ' + sql_builder.to_values(bulk_outputs_ids)

    ans.outputs_query = '' +
      'UPDATE\n' +
      '  outputs\n' +
      'SET\n' +
      '  "usedTxid" = inputs.input_txid,\n' +
      '  "usedBlockheight" = transactions.blockheight,\n' +
      '  used = TRUE\n' +
      'FROM\n' +
      '  inputs\n' +
      'JOIN transactions ON transactions.txid = inputs.input_txid\n' + 
      'WHERE\n' +
      '  (inputs.txid = outputs.txid AND inputs.vout = outputs.n) AND (' + outputs_conditions + ')'
  }

  if (bulk_inputs.length) {
    bulk_inputs.push({txid: 'ffff', vout: -1}) // ugly hack - postgres mistakenly uses seq scan when all vout are the same
    const inputs_conditions = bulk_inputs.map((input) => {
      return '(inputs.txid = ' + sql_builder.to_value(input.txid) + ' AND inputs.vout = ' + input.vout + ')'
    }).join(' OR ')

    ans.inputs_query = '' +
      'UPDATE\n' +
      '  inputs\n' +
      'SET\n' +
      '  output_id = outputs.id,\n' +
      '  value = outputs.value,\n' +
      '  fixed = TRUE\n' +
      'FROM\n' +
      '  outputs\n' +
      'WHERE\n' +
      '  (inputs.txid = outputs.txid AND inputs.vout = outputs.n) AND (' + inputs_conditions + ')'
  }

  if (!transactions || !transactions.length) {
    return ans
  }

  const transactions_updates = transactions.map((transaction) => {
    const set = {}
    // Oded: TODO - fix the issue where same fields should be updated
    set.tries = transaction.tries || 0
    set.fee = transaction.fee || 0
    set.totalsent = transaction.totalsent || 0
    set.iosparsed = transaction.iosparsed || false
    return {
      set: set,
      where: {txid: transaction.txid}
    }
  })
  ans.transactions_query = sql_builder.to_multi_condition_update({table_name: 'transactions', updates: transactions_updates})

  return ans
}

Scanner.prototype.parse_da = function (err, callback) {
  if (err) {
    console.error('parse_da: err = ', err)
    return this.parse_da()
  }
  const emits = []
  const cback = (err) => {
    this.parse_da(err);
  }
  callback = callback || cback

  this.get_next_block_to_da_parse(500, (err, raw_block_datas) => {
    if (err) return this.parse_da(err)
    if (!raw_block_datas || !raw_block_datas.length) {
      return setTimeout(() => {
        callback()
      }, 500)
    }
    const first_block = raw_block_datas[0].height
    const num_of_blocks = raw_block_datas.length
    const last_block = raw_block_datas[num_of_blocks - 1].height

    let did_work = false
    const close_blocks = (err, empty) => {
      if (debug) console.timeEnd('parse_da_bulks')
      if (err) return callback(err)
      emits.forEach((emit) => {
        this.emit(emit[0], emit[1])
      })
      if (!empty) {
        if (did_work) {
          return callback()
        } else {
          return setTimeout(() => {
            callback()
          }, 500)
        }
      }

      const blocks_heights = []
      raw_block_datas.forEach((raw_block_data) => {
        if (raw_block_data.txsparsed) {
          this.emit('newblock', raw_block_data)
          set_last_fully_parsed_block(raw_block_data.height)
          blocks_heights.push(raw_block_data.height)
        }
      })

      if (!blocks_heights.length) {
        return setTimeout(() => {
          callback()
        }, 500)
      }
      console.log('parse_da: close_blocks ' + blocks_heights[0] + '-' + blocks_heights[blocks_heights.length - 1])
      const update = {
        daparsed: true
      }
      const conditions = {
        height: {$in: blocks_heights}
      }
      this.Blocks.update(
        update,
        {
          where: conditions
        }
      ).then((res) => {
        console.log('parse_da - close_blocks - success')
        callback()
      }).catch((e) => {
        console.log('parse_da - close_blocks - e = ', e)
        callback(e)
      })
    }

    this.get_need_to_da_parse_transactions_by_blocks(first_block, last_block, (err, transactions_data) => {
      if (err) return this.parse_da(err)
      console.log('Parsing da for blocks ' + first_block + '-' + last_block + ' (' + transactions_data.length + ' txs).')
      if (!transactions_data) return callback('can\'t get transactions from db')
      if (!transactions_data.length) {
        if (debug) console.time('parse_da_bulks')
        return close_blocks(null, true)
      }

      async.each(transactions_data, (transaction_data, cb) => {
        const sql_query = []
        this.parse_da_tx(transaction_data, sql_query)
        if (!transaction_data.iosparsed) {
          return cb()
        }
        did_work = true
        sql_query.push(squel.update()
          .table('transactions')
          .set('daparsed', true)
          .set('overflow', transaction_data.overflow || false)
          .where('iosparsed AND txid = ?', transaction_data.txid)
          .toString())
        sql_query = sql_query.join(';\n')
        emits.push(['newdatransaction', transaction_data])
        emits.push(['newtransaction', transaction_data])
        this.sequelize.transaction((sql_transaction) => {
          return this.sequelize.query(sql_query, {transaction: sql_transaction})
            .then(() => { cb() })
            .catch(cb)
        })
      }, close_blocks)
    })
  })
}

Scanner.prototype.parse_da_tx = function (transaction_data, sql_query) {
  if (!transaction_data.iosparsed || !transaction_data.dadata || !transaction_data.dadata.length) {
    // console.warn('parse_cc_tx, problem: ', JSON.stringify(transaction_data))
    return
  }

  const assetsArrays = get_assets_outputs(transaction_data)
  assetsArrays.forEach((assetsArray, out_index) => {
    assetsArray = assetsArray || []
    transaction_data.vout[out_index].assets = assetsArray
    assetsArray.forEach((asset, index_in_output) => {
      let type = null
      if (transaction_data.dadata && transaction_data.dadata.length && transaction_data.dadata[0].type) {
        type = transaction_data.dadata[0].type
      }
      if (type === 'issuance') {
        sql_query.push(squel.insert()
          .into('assets')
          .set('assetId', asset.assetId)
          .set('lockStatus', asset.lockStatus)
          .set('divisibility', asset.divisibility)
          .set('aggregationPolicy', asset.aggregationPolicy)
          .toString() + ' ON CONFLICT ("assetId") DO NOTHING')
      }
      sql_query.push(squel.insert()
        .into('assetstransactions')
        .set('assetId', asset.assetId)
        .set('txid', transaction_data.txid)
        .set('type', type)
        .toString() + ' ON CONFLICT ("assetId", txid) DO NOTHING')
      sql_query.push(squel.insert()
        .into('assetsoutputs')
        .set('assetId', asset.assetId)
        .set('issueTxid', asset.issueTxid)
        .set('amount', asset.amount)
        .set('output_id', transaction_data.vout[out_index].id ||
          squel.select().field('id').from('outputs').where('txid = ? AND n = ?', transaction_data.txid, out_index))  // if no id (such as in the mempool case), go and fetch it
        .set('index_in_output', index_in_output)
        .toString() + ' ON CONFLICT ("assetId", output_id, index_in_output) DO NOTHING')
      if (asset.amount && transaction_data.vout[out_index].scriptPubKey && transaction_data.vout[out_index].scriptPubKey.addresses) {
        transaction_data.vout[out_index].scriptPubKey.addresses.forEach((address) => {
          sql_query.push(squel.insert()
            .into('assetsaddresses')
            .set('assetId', asset.assetId)
            .set('address', address)
            .toString() + ' ON CONFLICT ("assetId", address) DO NOTHING')
        })
      }
    })
  })
}

Scanner.prototype.get_need_to_da_parse_transactions_by_blocks = function (first_block, last_block, callback) {
  console.log('get_need_to_da_parse_transactions_by_blocks for blocks ' + first_block + '-' + last_block)
  const query = get_find_transaction_query(this,
    'WHERE\n' +
    '  daparsed = FALSE AND\n' +
    '  colored = TRUE AND\n' +
    '  blockheight BETWEEN ' + first_block + ' AND ' + last_block + '\n' +
    'ORDER BY\n' +
    '  blockheight ASC,\n' +
    '  index_in_block ASC\n' +
    'LIMIT 1000;')
  this.sequelize.query(query, {type: this.sequelize.QueryTypes.SELECT})
    .then((transactions) => {
      console.log('get_need_to_da_parse_transactions_by_blocks - transactions.length = ', transactions.length)
      callback(null, transactions)
    })
    .catch((e) => {
      console.log('get_need_to_da_parse_transactions_by_blocks - e = ', e)
      callback(e)
    })
}

Scanner.prototype.get_need_to_fix_transactions_by_blocks = function (first_block, last_block, callback) {
  const conditions = {
    iosparsed: false,
    blockheight: {$between: [first_block, last_block]}
  }
  console.time('get_need_to_fix_transactions_by_blocks')
  const query = get_find_transaction_query(this,
    'WHERE\n' +
    '  transactions.iosparsed = FALSE AND\n' +
    '  transactions.blockheight BETWEEN ' + first_block + ' AND ' + last_block + '\n' +
    'ORDER BY\n' +
    '  transactions.blockheight ASC,\n' +
    '  transactions.index_in_block ASC\n' +
    'LIMIT 200;')
  this.sequelize.query(query, {type: this.sequelize.QueryTypes.SELECT})
    .then((transactions) => {
      console.timeEnd('get_need_to_fix_transactions_by_blocks')
      console.log('get_need_to_fix_transactions_by_blocks #1 - transactions.length = ', transactions.length)
      callback(null, transactions)
    })
    .catch((e) => {
      console.log('get_need_to_fix_transactions_by_blocks - e = ', e)
      callback(e)
    })
}

Scanner.prototype.fix_vin = function (raw_transaction_data, blockheight, bulk_outputs_ids, bulk_inputs, callback) {
  // fixing a transaction - we need to fulfill the condition where a transaction is iosparsed if and only if
  // all of its inputs are fixed.
  // An input is fixed when it is assigned with the output_id of its associated output, and the output is marked as used.
  // If the transaction is colored - this should happen only after this output's transaction is both iosparsed AND ccparsed.
  // Otherwise, it is enough for it to be in DB.

  const inputs_to_fix = {}
  let outputs_conditions
  let transactions_conditions
  let find_vin_transactions_query

  if (!raw_transaction_data.vin) {
    return callback('transaction ' + raw_transaction_data.txid + ' does not have vin.')
  }

  const end = (in_transactions) => {
    const inputs_to_fix_now = []
    in_transactions.forEach((in_transaction) => {
      in_transaction.vout.forEach((output) => {
        let input
        if (in_transaction.txid + ':' + output.n in inputs_to_fix) {
          input = inputs_to_fix[in_transaction.txid + ':' + output.n]
          input.previousOutput = output.scriptPubKey
          input.value = output.value
          input.output_id = output.id
          input.assets = output.assets
          inputs_to_fix_now.push(input)
          bulk_inputs.push({txid: input.txid, vout: input.vout})
          bulk_outputs_ids.push(input.output_id)
        }
      })
    })

    const all_fixed = (inputs_to_fix_now.length === Object.keys(inputs_to_fix).length)
    if (all_fixed) {
      calc_fee(raw_transaction_data)
      if (raw_transaction_data.fee < 0) {
        console.log('calc_fee < 0 !!!')
        console.log('calc_fee: ' + raw_transaction_data.txid + ', raw_transaction_data.fee = ', raw_transaction_data.fee)
        console.log('calc_fee: ' + raw_transaction_data.txid + ', raw_transaction_data.totalsent = ', raw_transaction_data.totalsent)
        console.log('calc_fee, ' + raw_transaction_data.txid + ', raw_transaction_data = ', raw_transaction_data)
      }
    } else {
      raw_transaction_data.tries = raw_transaction_data.tries || 0
      raw_transaction_data.tries++
      if (raw_transaction_data.tries > 1000) {
        console.warn('transaction', raw_transaction_data.txid, 'has un parsed inputs (', Object.keys(inputs_to_fix), ') for over than 1000 tries.')
      }
    }
    raw_transaction_data.iosparsed = all_fixed
    callback()
  }

  const is_input_fixed = (input) => {
    return input.coinbase || input.output_id
  }

  raw_transaction_data.vin.forEach((vin) => {
    if (!is_input_fixed(vin)) {
      inputs_to_fix[vin.txid + ':' + vin.vout] = vin
    }
  })

  if (!Object.keys(inputs_to_fix).length) {
    return end([])
  }

  inputs_to_fix['ffff:-1'] = true   // hack to avoid seq scan
  outputs_conditions = '(' + Object.keys(inputs_to_fix).map((txid_index) => {
    txid_index = txid_index.split(':')
    const txid = txid_index[0]
    const n = txid_index[1]
    return '(outputs.txid = ' + sql_builder.to_value(txid) + ' AND outputs.n = ' + n + ')'
  }).join(' OR ') + ')'
  delete inputs_to_fix['ffff:-1']

  if (raw_transaction_data.colored) {
    find_vin_transactions_query = '' +
      'SELECT\n' +
      '  outputs.*,\n' +
      '  to_json(array(\n' +
      '    SELECT assets FROM\n' +
      '      (SELECT\n' +
      '        assetsoutputs."assetId", assetsoutputs."amount", assetsoutputs."issueTxid",\n' +
      '        assets.*\n' +
      '      FROM\n' +
      '        assetsoutputs\n' +
      '      INNER JOIN assets ON assets."assetId" = assetsoutputs."assetId"\n' +
      '      WHERE assetsoutputs.output_id = outputs.id ORDER BY index_in_output)\n' +
      '    AS assets)) AS assets\n' +
      'FROM outputs\n' +
      'JOIN transactions ON transactions.txid = outputs.txid\n' +
      'WHERE ((transactions.colored = FALSE) OR (transactions.colored = TRUE AND transactions.iosparsed = TRUE AND transactions.daparsed = TRUE)) AND ' + outputs_conditions + ';'
  } else {
    find_vin_transactions_query = '' +
      'SELECT\n' +
      '  outputs.*\n' +
      'FROM outputs\n' +
      'WHERE ' + outputs_conditions + ';'
  }
  // console.time('find_vin_transactions_query ' + raw_transaction_data.txid)
  this.sequelize.query(find_vin_transactions_query, {type: self.sequelize.QueryTypes.SELECT})
    .then((vin_transactions) => {
      // console.timeEnd('find_vin_transactions_query ' + raw_transaction_data.txid)
      vin_transactions = _(vin_transactions)
        .groupBy('txid')
        .transform((result, vout, txid) => {
          result.push({txid: txid, vout: vout})
        }, [])
        .value()
      end(vin_transactions)
    })
    .catch(callback)
}

const calc_fee = function (raw_transaction_data) {
  let fee = 0
  let totalsent = 0
  let coinbase = false
  if ('vin' in raw_transaction_data && raw_transaction_data.vin) {
    raw_transaction_data.vin.forEach((vin) => {
      if ('coinbase' in vin && vin.coinbase) {
        coinbase = true
      }
      if (vin.value) {
        fee += vin.value
      }
    })
  }
  if (raw_transaction_data.vout) {
    raw_transaction_data.vout.forEach((vout) => {
      if ('value' in vout && vout.value) {
        fee -= vout.value
        totalsent += vout.value
      }
    })
  }
  raw_transaction_data.totalsent = totalsent
  raw_transaction_data.fee = coinbase ? 0 : fee
}

const set_next_hash = function (next_hash) {
  properties.next_hash = next_hash
}

const set_last_hash = function (last_hash) {
  properties.last_hash = last_hash
}

const set_last_block = function (last_block) {
  properties.last_block = last_block
  process.send({to: properties.roles.API, last_block: last_block})
}

const set_last_fully_parsed_block = function (last_fully_parsed_block) {
  properties.last_fully_parsed_block = last_fully_parsed_block
  process.send({to: properties.roles.API, last_fully_parsed_block: last_fully_parsed_block})
}

Scanner.prototype.get_next_block_to_da_parse = function (limit, callback) {
  const conditions = {
    daparsed: false
  }
  const attributes = [
    'height',
    'hash',
    'time',
    'size',
    'totalsent',
    'fees',
    'txlength',
    'txsparsed'
  ]
  this.Blocks.findAll({where: conditions, attributes: attributes, order: [['height', 'ASC']], limit: limit, raw: true})
    .then((blocks) => { callback(null, blocks) })
    .catch((e) => {
      console.error('get_next_block_to_da_parse: e = ', e)
      callback(e)
    })
}

Scanner.prototype.parse_new_mempool_transaction = function (raw_transaction_data, sql_query, emits, callback) {
  let transaction_data
  let did_work = false
  let blockheight = -1
  async.waterfall([
    (cb) => {
      // console.time('parse_new_mempool_transaction - #1 lookup in DB, txid = ' + raw_transaction_data.txid)
      const find_transaction_query = get_find_transaction_query(this, 'WHERE txid = :txid ;')
      this.sequelize.query(find_transaction_query, {replacements: {txid: raw_transaction_data.txid}, type: this.sequelize.QueryTypes.SELECT})
        .then((transactions) => { cb(null, transactions[0]) })
        .catch(cb)
    }, (l_transaction_data, cb) => {
      // console.timeEnd('parse_new_mempool_transaction - #1 lookup in DB, txid = ' + raw_transaction_data.txid)
      // console.time('parse_new_mempool_transaction - #2 get_block_height, txid = ' + raw_transaction_data.txid)
      transaction_data = l_transaction_data
      if (transaction_data) {
        raw_transaction_data = transaction_data
        // blockheight = raw_transaction_data.blockheight || -1
        cb(null, blockheight)
      } else {
        // console.log('parse_new_mempool_transaction: did not find in DB, parsing new tx: ' + raw_transaction_data.txid)
        did_work = true
        if (blockheight === -1 && raw_transaction_data.blockhash) {
          get_block_height(raw_transaction_data.blockhash, cb)
        } else {
          cb(null, blockheight)
        }
      }
    }, (l_blockheight, cb) => {
      blockheight = l_blockheight
      // console.timeEnd('parse_new_mempool_transaction - #2 get_block_height, txid = ' + raw_transaction_data.txid)
      // console.log('parse_new_mempool_transaction - #2 blockheight = ', blockheight)
      if (transaction_data) return cb()
      if (raw_transaction_data.time) {
        raw_transaction_data.time = raw_transaction_data.time * 1000
      }
      if (raw_transaction_data.blocktime) {
        raw_transaction_data.blocktime = raw_transaction_data.blocktime * 1000
      } else {
        raw_transaction_data.blocktime = Date.now()
      }
      raw_transaction_data.blockheight = blockheight

      this.parse_vin(raw_transaction_data, blockheight, sql_query)
      this.parse_vout(raw_transaction_data, blockheight, sql_query)
      raw_transaction_data.iosparsed = false
      raw_transaction_data.daparsed = false
      cb()
    }, (cb) => {
      if (raw_transaction_data.iosparsed) {
        // console.log('parse_new_mempool_transaction - #3.1 transaction.iosparsed = true, txid = ' + raw_transaction_data.txid)
        cb()
      } else {
        did_work = true
        // console.log('parse_new_mempool_transaction - #3.2 fix_vin')
        // console.time('parse_new_mempool_transaction - fix_vin ' + raw_transaction_data.txid)
        const bulk_outputs_ids = []
        const bulk_inputs = []
        this.fix_vin(raw_transaction_data, blockheight, bulk_outputs_ids, bulk_inputs, (err) => {
          if (err) return cb(err)
          // console.timeEnd('parse_new_mempool_transaction - fix_vin ' + raw_transaction_data.txid)
          const queries = get_fix_transactions_update_query(bulk_outputs_ids, bulk_inputs)
          sql_query.push(queries.outputs_query)
          sql_query.push(queries.inputs_query)
          cb()
        })
      }
    }, (cb) => {
      if (raw_transaction_data.daparsed) {
        // console.log('parse_new_mempool_transaction - #4.1 raw_transaction_data.ccparsed = true, txid = ', raw_transaction_data.txid)
        cb(null, null)
      } else {
        if (raw_transaction_data.iosparsed && raw_transaction_data.colored && !raw_transaction_data.daparsed) {
          // console.log('parse_new_mempool_transaction - #4.2 parse_cc_tx, txid = ', raw_transaction_data.txid)
          this.parse_da_tx(raw_transaction_data, sql_query)
          raw_transaction_data.daparsed = true
          did_work = true
        }
        if (did_work && raw_transaction_data.iosparsed) {
          emits.push(['newtransaction', raw_transaction_data])
          if (raw_transaction_data.colored) {
            emits.push(['newdatransaction', raw_transaction_data])
          }
        }
        if (did_work) {
          const update = {
            iosparsed: raw_transaction_data.iosparsed,
            daparsed: raw_transaction_data.daparsed,
            tries: raw_transaction_data.tries || 0
          }
          if (raw_transaction_data.fee) update.fee = raw_transaction_data.fee
          if (raw_transaction_data.totalsent) update.totalsent = raw_transaction_data.totalsent
          // put this query first because of outputs and inputs foreign key constraints, validate transaction in DB
          sql_query.unshift(squel.insert()
            .into('transactions')
            .setFields(to_sql_fields(raw_transaction_data, {exclude: ['vin', 'vout', 'confirmations', 'index_in_block']}))
            .toString() + ' ON CONFLICT (txid) DO UPDATE SET ' + sql_builder.to_update_string(update))
        }
        cb()
      }
    }
  ], (err) => {
    if (err) return callback(err)
    callback(null, did_work, raw_transaction_data.iosparsed, raw_transaction_data.colored, raw_transaction_data.daparsed)
  })
}

Scanner.prototype.parse_mempool_cargo = function (txids, callback) {
  const new_mempool_txs = []
  const command_arr = []
  const emits = []
  txids = _.uniq(txids)
  const ph_index = txids.indexOf('PH')
  if (ph_index !== -1) {
    txids.splice(ph_index, 1)
  }
  console.log('parsing mempool cargo (' + txids.length + ')')

  txids.forEach((txhash) => {
    command_arr.push({ method: 'getrawtransaction', params: [txhash, 1] })
  })

  bitcoin_rpc.cmd(command_arr, (raw_transaction_data, cb) => {
    // console.log('received result from bitcoind, raw_transaction_data.txid = ', raw_transaction_data.txid)
    const sql_query = []
    if (!raw_transaction_data) {
      console.log('Null transaction')
      return cb()
    }
    raw_transaction_data = to_discrete(raw_transaction_data)
    // console.time('parse_new_mempool_transaction time - ' + raw_transaction_data.txid)
    this.parse_new_mempool_transaction(raw_transaction_data, sql_query, emits, (err, did_work, iosparsed, colored, daparsed) => {
      if (err) return cb(err)
      // work may have been done in priority_parse in context of API
      new_mempool_txs.push({
        txid: raw_transaction_data.txid,
        iosparsed: iosparsed,
        colored: colored,
        daparsed: daparsed
      })
      if (!did_work) {
        return cb()
      }
      if (!sql_query.length) return cb()
      sql_query = sql_query.join(';\n')
      this.sequelize.transaction((sql_transaction) => {
        return this.sequelize.query(sql_query, {transaction: sql_transaction})
          .then(() => {
            // console.timeEnd('parse_new_mempool_transaction time - ' + raw_transaction_data.txid)
            cb()
          })
          .catch(cb)
      })
    })
  }, (err) => {
    if (err) {
      if ('code' in err && err.code === -5) {
        console.error('Can\'t find tx.')
      } else {
        console.error('parse_mempool_cargo: ', err)
        return callback(err)
      }
    }
    console.log('parsing mempool bulks')
    if (this.mempool_txs) {
      new_mempool_txs.forEach((mempool_tx) => {
        let found = false
        this.mempool_txs.forEach((self_mempool_tx) => {
          if (!found && mempool_tx.txid === self_mempool_tx.txid) {
            found = true
            self_mempool_tx.iosparsed = mempool_tx.iosparsed
            self_mempool_tx.colored = mempool_tx.colored
            self_mempool_tx.daparsed = mempool_tx.daparsed
          }
        })
        if (!found) {
          this.mempool_txs.push({
            txid: mempool_tx.txid,
            iosparsed: mempool_tx.iosparsed,
            colored: mempool_tx.colored,
            daparsed: mempool_tx.daparsed
          })
        }
      })
    }
    emits.forEach((emit) => {
      this.emit(emit[0], emit[1])
    })
    callback()
  })
}

Scanner.prototype.revert_txids = function (callback) {
  this.to_revert = _.uniq(this.to_revert)
  if (!this.to_revert.length) return callback()
  console.log('need to revert ' + this.to_revert.length + ' txs from mempool.')
  let n_batch = 100
  // async.whilst(function () { return self.to_revert.length },
    // function (cb) {
      const txids = this.to_revert.slice(0, n_batch)
      console.log('reverting txs (' + txids.length + ',' + this.to_revert.length + ')')

      // logger.debug('reverting '+block_data.tx.length+' txs.')
      const regular_txids = []
      const colored_txids = []
      const sql_query = []
      async.map(txids, (txid, cb) => {
        bitcoin_rpc.cmd('getrawtransaction', [txid], (err, raw_transaction_data) => {
          if (err || !raw_transaction_data || !raw_transaction_data.confirmations) {
            regular_txids.push(txid)
            this.revert_tx(txid, sql_query, (err, colored, revert_flags_txids) => {
              if (err) return cb(err)
              if (colored) {
                colored_txids.push(txid)
              }
              cb(null, revert_flags_txids)
            })
          } else {
            console.log('found tx that do not need to revert', txid)
            if (~this.to_revert.indexOf(txid)) {
              this.to_revert.splice(this.to_revert.indexOf(txid), 1)
            }
            // No need for now....
            // if blockhash
            // get blockheight
            // parse tx (first parse)
            // if not iosfixed - set block as not fixed
            // if colored and not cc_parsed - set block as not cc_parsed
            cb(null, [])
          }
        })
      }, (err, revert_flags_txids) => {
        if (err) return callback(err)
        revert_transactions_flags(revert_flags_txids, sql_query)
        sql_query = sql_query.join(';\n')
        this.sequelize.query(sql_query, {logging: console.log, benchmark: true})
          .then(() => {
            regular_txids.forEach((txid) => {
              this.emit('revertedtransaction', {txid: txid})
            })
            colored_txids.forEach((txid) => {
              this.emit('reverteddatransaction', {txid: txid})
            })
            this.to_revert = []
            callback()
          })
          .catch(callback)
      })
  //   },
  //   callback
  // )
}

Scanner.prototype.parse_new_mempool = function (callback) {
  const db_parsed_txids = []
  const db_unparsed_txids = []
  let new_txids
  let cargo_size

  console.log('parse_new_mempool')
  if (properties.scanner.mempool !== 'true') return callback()
  console.log('start reverting (if needed)')
  async.waterfall([
    (cb) => {
      this.revert_txids(cb)
    }, (cb) => {
      console.log('end reverting (if needed)')
      if (!this.mempool_txs) {
        this.emit('mempool')
        const conditions = {
          blockheight: -1
        }
        const attributes = ['txid', 'iosparsed', 'colored', 'daparsed']
        const limit = 10000
        let has_next = true
        let offset = 0
        this.mempool_txs = []
        async.whilst(() => { return has_next },
          (cb) => {
            console.time('find mempool db txs')
            this.Transactions.findAll({
              where: conditions,
              attributes: attributes,
              limit: limit,
              offset: offset,
              raw: true
            }).then((transactions) => {
              console.timeEnd('find mempool db txs')
              console.time('processing mempool db txs')
              this.mempool_txs = this.mempool_txs.concat(transactions)
              transactions.forEach((transaction) => {
                if (transaction.iosparsed && transaction.colored === transaction.daparsed) {
                  db_parsed_txids.push(transaction.txid)
                } else {
                  db_unparsed_txids.push(transaction.txid)
                }
              })
              console.timeEnd('processing mempool db txs')
              if (transactions.length === limit) {
                console.log('getting txs', offset + 1, '-', offset + limit)
                offset += limit
              } else {
                has_next = false
              }
              cb()
            }).catch(cb)
          },
        cb)
      } else {
        console.log('getting mempool from memory cache')
        this.mempool_txs.forEach((transaction) => {
          if (transaction.iosparsed && transaction.colored === transaction.daparsed) {
            db_parsed_txids.push(transaction.txid)
          } else {
            db_unparsed_txids.push(transaction.txid)
          }
        })
        cb()
      }
    }, (cb) => {
      console.log('start find mempool digibyted txs')
      bitcoin_rpc.cmd('getrawmempool', [], cb)
    }, (whole_txids, cb) => {
      whole_txids = whole_txids || []
      console.log('end find mempool digibyted txs')
      console.log('parsing mempool txs (' + whole_txids.length + ')')
      console.log('start xoring')
      // console.log('db_parsed_txids = ', db_parsed_txids.map(function (txid) { return txid }))
      const txids_intersection = _.intersection(db_parsed_txids, whole_txids) // txids that allready parsed in db
      // console.log('txids_intersection = ', txids_intersection.map(function (txid) { return txid }))
      new_txids = _.xor(txids_intersection, whole_txids) // txids that not parsed in db
      // console.log('new_txids = ', new_txids.map(function (txid) { return txid }))
      db_parsed_txids = _.xor(txids_intersection, db_parsed_txids) // the rest of the txids in the db (not yet found in mempool)
      txids_intersection = _.intersection(db_unparsed_txids, whole_txids) // txids that in mempool and db but not fully parsed
      db_unparsed_txids = _.xor(txids_intersection, db_unparsed_txids) // the rest of the txids in the db (not yet found in mempool, not fully parsed)
      console.log('end xoring')
      new_txids.push('PH')
      console.log('parsing new mempool txs (' + (new_txids.length - 1) + ')')
      cargo_size = new_txids.length
      const ended = 0
      const end_func = () => {
        if (!ended++) {
          this.removeListener('kill', end_func)
          console.log('mempool cargo ended.')
          cb()
        }
      }
      this.once('kill', end_func)
      this.mempool_cargo.push(new_txids, () => {
        if (!--cargo_size) {
          const db_txids = db_parsed_txids.concat(db_unparsed_txids)
          this.to_revert = this.to_revert.concat(db_txids)
          _.pullAllWith(this.mempool_txs, db_txids, (tx, txid) => {
            return tx.txid === txid
          })
          end_func()
        }
      })
    }
  ], callback)
}

Scanner.prototype.wait_for_parse = function (txid, callback) {
  let sent = 0
  const end = (err) => {
    if (!sent++) {
      callback(err)
    }
  }

  const listener = (transaction) => {
    if (transaction.txid === txid) {
      this.removeListener('newtransaction', listener)
      end()
    }
  }
  this.on('newtransaction', listener)

  const conditions = {
    txid: txid,
    iosparsed: true,
    daparsed: {$col: 'colored'}
  }
  const attributes = ['txid']
  this.Transactions.findOne({ where: conditions, attributes: attributes, raw: true })
  .then((transaction) => {
    if (transaction) end()
  })
  .catch(end)
}

Scanner.prototype.priority_parse = function (txid, callback) {
  const PARSED = 'PARSED'
  let transaction
  console.log('start priority_parse: ' + txid)
  console.time('priority_parse time: ' + txid)
  const end = (err) => {
    if (~this.priority_parse_list.indexOf(txid)) {
      this.priority_parse_list.splice(this.priority_parse_list.indexOf(txid), 1)
    }
    console.timeEnd('priority_parse time: ' + txid)
    callback(err)
  }

  async.waterfall([
    (cb) => {
      if (~this.priority_parse_list.indexOf(txid)) {
        return this.wait_for_parse(txid, (err) => {
          if (err) return cb(err)
          cb(PARSED)
        })
      }
      console.time('priority_parse: find in db ' + txid)
      this.priority_parse_list.push(txid)
      const conditions = {
        txid: txid,
        iosparsed: true,
        daparsed: {$col: 'colored'}
      }
      const attributes = ['txid']
      this.Transactions.findOne({ where: conditions, attributes: attributes, raw: true })
      .then((tx) => { cb(null, tx) })
      .catch(cb)
    }, (tx, cb) => {
      console.timeEnd('priority_parse: find in db ' + txid)
      if (tx) return cb(PARSED)
      console.time('priority_parse: get_from_digibyted ' + txid)
      bitcoin_rpc.cmd('getrawtransaction', [txid, 1], (err, raw_transaction_data) => {
        if (err && err.code === -5) return cb(['tx ' + txid + ' not found.', 204])
        cb(err, raw_transaction_data)
      })
    }, (raw_transaction_data, cb) => {
      console.timeEnd('priority_parse: get_from_digibyted ' + txid)
      console.time('priority_parse: parse inputs ' + txid)
      transaction = raw_transaction_data
      transaction = to_discrete(transaction)
      if (!transaction || !transaction.vin) return cb(['tx ' + txid + ' not found.', 204])
      async.each(transaction.vin, (vin, cb2) => {
        this.priority_parse(vin.txid, cb2)
      },
      cb)
    }, (cb) => {
      console.timeEnd('priority_parse: parse inputs ' + txid)
      console.time('priority_parse: parse ' + txid)
      this.mempool_cargo.unshift(txid, cb)
    }
  ], (err) => {
    if (err) {
      if (err === PARSED) {
        return end()
      }
      return end(err)
    }
    console.timeEnd('priority_parse: parse ' + txid)
    end()
  })
}

Scanner.prototype.get_info = function (callback) {
  bitcoin_rpc.cmd('getblockchaininfo', [], callback)
}

const is_coinbase = function (tx_data) {
  return (tx_data && tx_data.vin && tx_data.vin.length === 1 && tx_data.vin[0].coinbase)
}

const to_discrete = function (raw_transaction_data) {
  if (!raw_transaction_data || !raw_transaction_data.vout) return raw_transaction_data

  raw_transaction_data.vout.forEach((vout) => {
    if (vout.value) {
      vout.value *= 100000000
    }
  })
  return raw_transaction_data
}

Scanner.prototype.transmit = function (txHex, callback) {
  if (typeof txHex !== 'string') {
    txHex = txHex.toHex()
  }
  bitcoin_rpc.cmd('sendrawtransaction', [txHex], (err, txid) => {
    if (err) return callback(err)
    this.priority_parse(txid, (err) => {
      if (err) return callback(err)
      return callback(null, {txid: txid})
    })
  })
}

const get_find_transaction_query = function (self, query_tail) {
  return '' +
    'SELECT\n' +
    '  ' + sql_builder.to_columns_of_model(self.Transactions, {exclude: ['hex']}) + ',\n' +
    '  to_json(array(\n' +
    '    SELECT\n' +
    '      vin\n' +
    '    FROM\n' +
    '      (SELECT\n' +
    '       inputs.*,\n' +
    '       "previousOutput"."scriptPubKey" AS "previousOutput",\n' +
    '       to_json(array(\n' +
    '          SELECT\n' +
    '            assets\n' +
    '          FROM\n' +
    '            (SELECT\n' +
    '              assetsoutputs."assetId", assetsoutputs."amount", assetsoutputs."issueTxid",\n' +
    '              assets.*\n' +
    '            FROM\n' +
    '              assetsoutputs\n' +
    '            INNER JOIN assets ON assets."assetId" = assetsoutputs."assetId"\n' +
    '            WHERE assetsoutputs.output_id = inputs.output_id ORDER BY index_in_output)\n' +
    '        AS assets)) AS assets\n' +
    '      FROM\n' +
    '        inputs\n' +
    '      LEFT OUTER JOIN\n' +
    '        (SELECT outputs.id, outputs."scriptPubKey"\n' +
    '         FROM outputs) AS "previousOutput" ON "previousOutput".id = inputs.output_id\n' +
    '      WHERE\n' +
    '        inputs.input_txid = transactions.txid\n' +
    '      ORDER BY input_index) AS vin)) AS vin,\n' +
    '  to_json(array(\n' +
    '    SELECT\n' +
    '      vout\n' +
    '    FROM\n' +
    '      (SELECT\n' +
    '        outputs.*,\n' +
    '        to_json(array(\n' +
    '         SELECT assets FROM\n' +
    '           (SELECT\n' +
    '              assetsoutputs."assetId", assetsoutputs."amount", assetsoutputs."issueTxid",\n' +
    '              assets.*\n' +
    '            FROM\n' +
    '              assetsoutputs\n' +
    '            INNER JOIN assets ON assets."assetId" = assetsoutputs."assetId"\n' +
    '            WHERE assetsoutputs.output_id = outputs.id ORDER BY index_in_output)\n' +
    '        AS assets)) AS assets\n' +
    '      FROM\n' +
    '        outputs\n' +
    '      WHERE outputs.txid = transactions.txid\n' +
    '      ORDER BY n) AS vout)) AS vout\n' +
    'FROM\n' +
    '  transactions\n' + query_tail
}

/**
 * @param {object} key-value pairs for a table insert
 * @param {object} [options={}]
 * @param {object} [options.exclude] array of keys to exclude from the given object
 * @return {object} key-value pairs for a table insert
*/
const to_sql_fields = function (obj, options) {
  const ans = {}
  Object.keys(obj).forEach((key) => {
    if (!options || !options.exclude || options.exclude.indexOf(key) === -1) {
      if (typeof obj[key] === 'object') {
        ans[key] = JSON.stringify(obj[key])
        if (ans[key] === 'null') delete ans[key]
      } else {
        ans[key] = obj[key]
      }
    }
  })
  return ans
}

module.exports = Scanner
