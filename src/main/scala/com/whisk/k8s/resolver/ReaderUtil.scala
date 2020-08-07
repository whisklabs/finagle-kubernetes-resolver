package com.whisk.k8s.resolver

import com.twitter.io.{Buf, Reader}
import com.twitter.util._

object ReaderUtil {

  /**
    * Read infinite streaming response
    * This future will be resolved only in case of error
    */
  def readStream[T](reader: Reader[Buf], parseChunk: Buf => Either[Throwable, T])(
      callback: T => Unit
  ): Future[Unit] = {
    val promise = Promise[Unit]()

    def terminate(e: Throwable): Unit = {
      reader.discard()
      promise.setException(e)
    }

    def readNextChunk(): Unit = {
      reader
        .read()
        .onSuccess {
          case Some(buf) =>
            parseChunk(buf) match {
              case Left(e) =>
                terminate(e)

              case Right(value) =>
                callback(value)
                // recursive call to continue reading from stream
                readNextChunk()
            }
          case None =>
            terminate(new Exception("Stream end"))
        }
        .onFailure(error => {
          promise.setException(error)
        })
    }

    readNextChunk()
    promise
  }
}
