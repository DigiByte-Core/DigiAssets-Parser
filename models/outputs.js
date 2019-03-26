'use strict'

var DigiAssetDataTypes = require('./digiAssetDataTypes')

module.exports = function (sequelize, DataTypes) {
  var Outputs = sequelize.define('outputs', {
    id: {
      type: DataTypes.BIGINT,
      primaryKey: true,
      autoIncrement: true
    },
    txid: {
      type: DigiAssetDataTypes.HASH,
      unique: 'output_id'
    },
    n: {
      type: 'SMALLINT',
      unique: 'output_id'
    },
    used: {
      type: DataTypes.BOOLEAN,
      defaultValue: false
    },
    usedTxid: {
      type: DigiAssetDataTypes.HASH
    },
    usedBlockheight: {
      type: DataTypes.INTEGER
    },
    value: {
      type: DataTypes.BIGINT
    },
    scriptPubKey: {
      type: DataTypes.JSONB  // to contain hex, asm, type [, reqSigs, addresses]
    }
  },
  {
    classMethods: {
      associate: function (models) {
        Outputs.belongsTo(models.transactions, { foreignKey: 'txid', as: 'transaction' })
        Outputs.hasOne(models.inputs, { foreignKey: 'output_id', constraints: false }) // constraints = false because an output is not necessarily used
        Outputs.hasMany(models.addressesoutputs, { foreignKey: 'output_id', constraints: false }) // constraints = false because addressoutput is inserted after output
        Outputs.hasMany(models.assetsoutputs, { foreignKey: 'output_id', as: 'assetsoutputs', constraints: false })
      }
    },
    indexes: [
      {
        fields: ['txid']
      },
      {
        fields: ['txid', 'n', 'used']
      },
      {
        fields: ['id', 'used']
      }
    ],
    timestamps: false
  })

  return Outputs
}
