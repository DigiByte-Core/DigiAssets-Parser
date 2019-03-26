'use strict'

var DigiAssetDataTypes = require('./digiAssetDataTypes')

module.exports = function (sequelize, DataTypes) {
  var Transactions = sequelize.define('transactions', {
    blockheight: {
      type: DataTypes.INTEGER
    },
    blockhash: {
      type: DigiAssetDataTypes.HASH
    },
    hash: {
      type: DigiAssetDataTypes.HASH
    },
    blocktime: {
      type: DataTypes.BIGINT
    },
    index_in_block: {
      type: 'SMALLINT'
    },
    txid: {
      type: DigiAssetDataTypes.HASH,
      primaryKey: true
    },
    hex: {
      type: DataTypes.TEXT
    },
    size: {
      type: DataTypes.INTEGER
    },
    vsize: {
      type: DataTypes.INTEGER
    },
    weight: {
      type: DataTypes.INTEGER
    },
    version: {
      type: DataTypes.INTEGER
    },
    locktime: {
      type: DataTypes.BIGINT
    },
    time: {
      type: DataTypes.BIGINT
    },
    fee: {
      type: DataTypes.BIGINT
    },
    totalsent: {
      type: DataTypes.BIGINT
    },
    overflow: {
      type: DataTypes.BOOLEAN
    },
    dadata: {
      type: DataTypes.JSONB
    },
    colored: {
      type: DataTypes.BOOLEAN,
      defaultValue: false
    },
    iosparsed: {
      type: DataTypes.BOOLEAN,
      defaultValue: false
    },
    daparsed: {
      type: DataTypes.BOOLEAN,
      defaultValue: false
    },
    tries: {
      type: DataTypes.INTEGER,
      defaultValue: 0
    }
  },
  {
    validate: {
      blockProperties: function () {
        if ((this.blockheight > -1) !== (this.index_in_block !== null)) {
          throw new Error('Require index_in_block when in block')
        }
      }
    },
    classMethods: {
      associate: function (models) {
        Transactions.hasMany(models.outputs, { foreignKey: 'txid', as: 'vout' })
        Transactions.hasMany(models.inputs, { foreignKey: 'input_txid', as: 'vin' }) // inputs in this transaction
        Transactions.belongsToMany(models.assets, { as: 'assets', through: models.assetstransactions, foreignKey: 'txid', otherKey: 'assetId' })
      }
    },
    indexes: [
      {
        fields: ['blockheight']
      },
      {
        fields: ['blockhash']
      },
      {
        fields: ['blocktime']
      },
      {
        fields: ['iosparsed']
      },
      {
        fields: ['overflow']
      },
      {
        fields: ['colored']
      },
      {
        fields: ['daparsed']
      },
      {
        fields: ['blockheight', 'iosparsed']
      },
      {
        fields: ['blockheight', 'colored']
      },
      {
        fields: ['blockheight', 'colored', 'daparsed']
      },
      {
        fields: ['blockheight', 'index_in_block']
      }
    ],
    timestamps: false
  })

  return Transactions
}
