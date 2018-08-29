package io.iohk.ethereum.db.postgres

import akka.util.ByteString
import io.getquill.{PostgresJdbcContext, SnakeCase}
import io.iohk.ethereum.db.Models.{BlockHeaderModel, OmmerModel, TransactionModel, hashToString}
import io.iohk.ethereum.db.storage.TransactionMappingStorage.TransactionLocation
import io.iohk.ethereum.domain.{Block, BlockHeader, SignedTransaction}
import io.iohk.ethereum.network.p2p.messages.PV62.BlockBody


trait PostgresStorage {

  protected val ctx: PostgresJdbcContext[SnakeCase.type]


  import ctx._

  val BlockHeaders = quote(
    querySchema[BlockHeaderModel]("block_headers")
  )

  val Transactions = quote(
    querySchema[TransactionModel]("transactions")
  )

  val Ommers = quote(
    querySchema[OmmerModel]("ommers")
  )


  implicit val blockHeadersInsertMeta = insertMeta[BlockHeaderModel]()

  def saveBlockHeader(blockHeader: BlockHeader): Long =
    ctx.run(BlockHeaders.insert(lift(BlockHeaderModel.toModel(blockHeader))))

  def saveBlock(block: Block): Unit = ctx.transaction {
    saveBlockHeader(block.header)
    saveBlockBody(block.header.hash, block.body)
  }

  def getBlockByNumber(number: BigInt): Option[Block] = ???

  def getBlockByHash(hash: ByteString): Option[Block] =
    for {
      header <- getBlockHeaderByHash(hash)
      body = getBlockBodyByHash(header.hash)
    } yield Block(header, body.getOrElse(BlockBody(Nil, Nil)))

  def getTransactionIndex(): Option[TransactionLocation] = ???

  def saveBlockBody(blockHash: ByteString, blockBody: BlockBody): Unit =
    ctx.transaction {
      saveBodyQuery(blockHash, blockBody)
    }

  def getBlockHeaderByNumber(number: BigInt): Option[BlockHeader] =
    ctx.run(BlockHeaders.filter(_.number == lift(number.toString()))).headOption.map(BlockHeaderModel.fromModel)

  def getBlockHeaderByHash(hash: ByteString): Option[BlockHeader] =
    ctx.run(BlockHeaders.filter(_.hash == lift(hashToString(hash)))).headOption.map(BlockHeaderModel.fromModel)

  def getBlockBodyByHash(hash: ByteString): Option[BlockBody] = {
    val blockHash = hashToString(hash)

    val txs = ctx.run(
      Transactions.filter(t => t.blockHash == lift(blockHash))
    ).map(TransactionModel.fromModel)

    val ommers = ctx.run(
      Ommers.filter(o => o.blockHash == lift(blockHash))
    ).map(OmmerModel.fromModel)

    if (ommers.isEmpty && txs.isEmpty) None
    else Some(BlockBody(txs, ommers))
  }

  def removeBlock(blockHash: ByteString): Long =
    ctx.run(BlockHeaders.filter(b => b.hash == lift(hashToString(blockHash))).delete)

  private def saveBodyQuery(blockHash: ByteString, blockBody: BlockBody) = {
    val convertTxToModel: (SignedTransaction) => TransactionModel = TransactionModel.toModel(blockHash, _)
    val convertOmmersToModel: (BlockHeader) => OmmerModel = OmmerModel.toModel(blockHash, _)
    quote {
      liftQuery(blockBody.transactionList.map(convertTxToModel)).foreach(t => Transactions.insert(t))
      liftQuery(blockBody.uncleNodesList.map(convertOmmersToModel)).foreach(o => Ommers.insert(o))
    }
  }

}

object PostgresStorage {


}
