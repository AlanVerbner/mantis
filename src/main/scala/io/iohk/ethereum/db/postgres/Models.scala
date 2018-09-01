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

  implicit val addressToString: Transformer[Address, String] = _.toUnprefixedString

  implicit val stringToAddress: Transformer[String, Address] = Address(_)

  def hashToString(b: ByteString): String = byteStringToString.transform(b)

  type Hash = String

  case class BlockHeaderModel(
    hash: String,
    parentHash: String,
    ommersHash: String,
    beneficiary: String,
    stateRoot: String,
    transactionsRoot: String,
    receiptsRoot: String,
    logsBloom: String,
    difficulty: BigInt,
    number: BigInt,
    gasLimit: BigInt,
    gasUsed: BigInt,
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
    hash: String,
    blockHash: String,
    nonce: BigInt,
    gasPrice: BigInt,
    gasLimit: BigInt,
    receivingAddress: Option[String],
    value: BigInt,
    payload: String,
    pointSign: String,
    signatureRandom: BigInt,
    signature: BigInt,
    senderAddress: String
  )

  object TransactionModel {
    def toModel(blockHash: ByteString, signedTransaction: SignedTransaction): TransactionModel =
      signedTransaction.tx.into[TransactionModel]
        .withFieldConst(_.hash, byteStringToString.transform(signedTransaction.hash))
        .withFieldConst(_.blockHash, byteStringToString.transform(blockHash))
        .withFieldConst(_.pointSign, signedTransaction.signature.v.toChar.toString)
        .withFieldConst(_.signatureRandom, signedTransaction.signature.r)
        .withFieldConst(_.signature, signedTransaction.signature.s)
        .withFieldConst(_.senderAddress, signedTransaction.senderAddress.toUnprefixedString)
        .transform

    def fromModel(transactionModel: TransactionModel): SignedTransaction = {
      val tx = transactionModel.into[Transaction]
        .transform
      val senderAddress = Address(transactionModel.senderAddress)
      SignedTransaction(
        tx,
        transactionModel.pointSign.toByte,
        transactionModel.signatureRandom,
        transactionModel.signature,
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
    difficulty: BigInt,
    number: BigInt,
    gasLimit: BigInt,
    gasUsed: BigInt,
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

