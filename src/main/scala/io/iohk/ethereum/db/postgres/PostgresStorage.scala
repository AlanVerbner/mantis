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
    querySchema[BlockHeaderModel]("public.block_headers")
  )

  val Transactions = quote(
    querySchema[TransactionModel]("public.transactions")
  )

  val Ommers = quote(
    querySchema[OmmerModel]("public.ommers")
  )

  lazy implicit val bigIntDecoder: Decoder[BigInt] =
    decoder((index, row) =>
      BigInt(row.getObject(index).toString.trim))

  lazy implicit val bigIntEncoder: Encoder[BigInt] =
    encoder(java.sql.Types.BIGINT, (index, value, row) =>
      row.setObject(index, value, java.sql.Types.BIGINT))

  implicit val blockHeadersInsertMeta = insertMeta[BlockHeaderModel]()

  def saveBlockHeader(blockHeader: BlockHeader): Long =
    ctx.run(BlockHeaders.insert(lift(BlockHeaderModel.toModel(blockHeader))).onConflictIgnore)

  def saveBlock(block: Block): Unit = ctx.transaction {
    saveBlockHeader(block.header)
    saveBlockBody(block.header.hash, block.body)
  }

  def getBlockByNumber(number: BigInt): Option[Block] =
    for {
      header <- getBlockHeaderByNumber(number)
      body = getBlockBodyByNumber(number)
    } yield Block(header, body.getOrElse(BlockBody(Nil, Nil)))

  def getBlockByHash(hash: ByteString): Option[Block] =
    for {
      header <- getBlockHeaderByHash(hash)
      body = getBlockBodyByHash(header.hash)
    } yield Block(header, body.getOrElse(BlockBody(Nil, Nil)))

  def getTransactionIndex(): Option[TransactionLocation] = ???

  def saveBlockBody(blockHash: ByteString, blockBody: BlockBody): Unit =
    ctx.transaction {
      ctx.run(saveTransactionsQuery(blockHash, blockBody))
      ctx.run(saveOmmersQuery(blockHash, blockBody))
    }

  def getBlockHeaderByNumber(number: BigInt): Option[BlockHeader] =
    ctx.run(findBlockHeaderByNumberQuery(number)).headOption.map(BlockHeaderModel.fromModel)

  def getBlockHeaderByHash(hash: ByteString): Option[BlockHeader] =
    ctx.run(findBlockHeaderByHashQuery(hash)).headOption.map(BlockHeaderModel.fromModel)

  def getBlockBodyByHash(hash: ByteString): Option[BlockBody] = {
    val blockHash = hashToString(hash)

    val txs = ctx.run(findTransactionsByBlockHashQuery(blockHash)).map(TransactionModel.fromModel)
    val ommers = ctx.run(findOmmersByBlockHashQuery(blockHash)).map(OmmerModel.fromModel)

    asMaybeBlockBody(txs, ommers)
  }

  def getBlockBodyByNumber(number: BigInt): Option[BlockBody] = {
    val txs = ctx.run(findTransactionsByBlockNumberQuery(number)).map(TransactionModel.fromModel)
    val ommers = ctx.run(findOmmersByBlockNumberQuery(number)).map(OmmerModel.fromModel)

    asMaybeBlockBody(txs, ommers)
  }

  def removeBlock(blockHash: ByteString): Long = ctx.run(deleteBlockQuery(blockHash))

  private def saveTransactionsQuery(blockHash: ByteString, blockBody: BlockBody) = {
    val convertTxToModel: (SignedTransaction) => TransactionModel = TransactionModel.toModel(blockHash, _)
    quote {
      liftQuery(blockBody.transactionList.map(convertTxToModel)).foreach(t => Transactions.insert(t).onConflictIgnore)
    }
  }

  private def saveOmmersQuery(blockHash: ByteString, blockBody: BlockBody) = {
    val convertOmmersToModel: (BlockHeader) => OmmerModel = OmmerModel.toModel(blockHash, _)
    quote {
      liftQuery(blockBody.uncleNodesList.map(convertOmmersToModel)).foreach(o => Ommers.insert(o).onConflictIgnore)
    }
  }

  private def findBlockHeaderByHashQuery(hash: ByteString) = quote {
    BlockHeaders.filter(_.hash == lift(hashToString(hash)))
  }

  private def deleteBlockQuery(blockHash: ByteString) = quote {
    BlockHeaders
      .filter(b => b.hash == lift(hashToString(blockHash)))
      .delete
  }

  private def findBlockHeaderByNumberQuery(number: BigInt) = quote {
    BlockHeaders.filter(_.number == lift(number))
  }

  private def findTransactionsByBlockHashQuery(blockHash: String) = quote {
    Transactions.filter(t => t.blockHash == lift(blockHash))
  }


  private def findOmmersByBlockHashQuery(blockHash: String) = quote {
    Ommers.filter(o => o.blockHash == lift(blockHash))
  }

  private def findTransactionsByBlockNumberQuery(blockNumber: BigInt) = quote {
    BlockHeaders
      .join(Transactions)
      .on((bh, t) => bh.hash == t.blockHash)
      .filter(_._1.number == lift(blockNumber))
      .map(_._2)
  }

  private def findOmmersByBlockNumberQuery(blockNumber: BigInt) = quote {
    BlockHeaders
      .join(Ommers)
      .on((bh, o) => bh.hash == o.blockHash)
      .filter(_._1.number == lift(blockNumber))
      .map(_._2)
  }

  private def asMaybeBlockBody(txs: Seq[SignedTransaction], ommers: Seq[BlockHeader]): Option[BlockBody] =
    if (txs.isEmpty && ommers.isEmpty) None
    else Some(BlockBody(txs, ommers))
}

object PostgresStorage {


}
