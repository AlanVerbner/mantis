package io.iohk.ethereum.db

import akka.util.ByteString
import io.iohk.ethereum.domain.{Address, BlockHeader, SignedTransaction, Transaction}
import org.spongycastle.util.encoders.Hex


object Models {

  import io.scalaland.chimney.Transformer
  import io.scalaland.chimney.dsl._

  implicit val byteStringToString: Transformer[ByteString, String] =
    (b: ByteString) => Hex.toHexString(b.toArray[Byte])

  implicit val stringToByteString: Transformer[String, ByteString] =
    (s: String) => ByteString.fromArray(Hex.decode(s))

  implicit val bigIntToString: Transformer[BigInt, String] = _.toString

  implicit val stringToBigInt: Transformer[String, BigInt] = s => BigInt(s)

  implicit val addressToString: Transformer[Address, String] = _.toString

  implicit val stringToAddress: Transformer[String, Address] = Address(_)

  def hashToString(b: ByteString): String = byteStringToString.transform(b)

  def blockNumberToString(n: BigInt): String = bigIntToString.transform(n)

  case class BlockHeaderModel(
    hash: String,
    parentHash: String,
    ommersHash: String,
    beneficiary: String,
    stateRoot: String,
    transactionsRoot: String,
    receiptsRoot: String,
    logsBloom: String,
    difficulty: String,
    number: String,
    gasLimit: String,
    gasUsed: String,
    unixTimestamp: Long,
    extraData: String,
    mixHash: String,
    nonce: String)

  object BlockHeaderModel {

    def toModel(blockHeader: BlockHeader): BlockHeaderModel = blockHeader.into[BlockHeaderModel]
      .withFieldComputed(_.hash, _.hashAsHexString)
      .transform

    def fromModel(blockHeaderModel: BlockHeaderModel): BlockHeader = blockHeaderModel.into[BlockHeader].transform
  }

  case class TransactionModel(
    blockHash: String,
    nonce: String,
    gasPrice: String,
    gasLimit: String,
    receivingAddress: Option[String],
    value: String,
    payload: String,
    pointSign: Byte,
    signatureRandom: String,
    signature: String,
    senderAddress: String
  )

  object TransactionModel {
    def toModel(blockHash: ByteString, signedTransaction: SignedTransaction): TransactionModel =
      signedTransaction.tx.into[TransactionModel]
        .withFieldConst(_.blockHash, byteStringToString.transform(blockHash))
        .withFieldConst(_.pointSign, signedTransaction.signature.v)
        .withFieldConst(_.signatureRandom, bigIntToString.transform(signedTransaction.signature.r))
        .withFieldConst(_.signature, bigIntToString.transform(signedTransaction.signature.s))
        .withFieldConst(_.senderAddress, signedTransaction.senderAddress.toString)
        .transform

    def fromModel(transactionModel: TransactionModel): SignedTransaction = {
      val tx = transactionModel.into[Transaction]
        .transform
      val senderAddress = Address(transactionModel.senderAddress)
      SignedTransaction(
        tx,
        transactionModel.pointSign,
        stringToByteString.transform(transactionModel.signatureRandom),
        stringToByteString.transform(transactionModel.signature),
        senderAddress)
    }
  }

  case class OmmerModel(
    blockHash: String,
    hash: String,
    parentHash: String,
    ommersHash: String,
    beneficiary: String,
    stateRoot: String,
    transactionsRoot: String,
    receiptsRoot: String,
    logsBloom: String,
    difficulty: String,
    number: String,
    gasLimit: String,
    gasUsed: String,
    unixTimestamp: Long,
    extraData: String,
    mixHash: String,
    nonce: String)

  object OmmerModel {

    def toModel(blockHash: ByteString, blockHeader: BlockHeader): OmmerModel = blockHeader.into[OmmerModel]
      .withFieldConst(_.blockHash, byteStringToString.transform(blockHash))
      .withFieldComputed(_.hash, _.hashAsHexString)
      .transform

    def fromModel(ommersModel: OmmerModel): BlockHeader = ommersModel.into[BlockHeader].transform
  }

}

