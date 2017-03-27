module internal Comm

open System
open System.IO
open System.Reflection
open System.Diagnostics
open System.Text
open NNanomsg

let maxMessageSize = 128

type TMsg =
    | Msg of int32 * string
    | Error of int32 * string
    static member Empty = Msg (0, String.Empty)

let Emptym = TMsg.Empty

let serialize (msg:TMsg) =
    match msg with
    | Error(errn, errm) -> [||]
    | Msg(logicId, note) ->
        let nsize = Encoding.UTF8.GetByteCount(note)
        let size = nsize + 8
        let bytes:byte array = Array.zeroCreate size
        let nbytes = Encoding.UTF8.GetBytes(note)
        Buffer.BlockCopy(BitConverter.GetBytes(size), 0, bytes, 0, 4)
        Buffer.BlockCopy(BitConverter.GetBytes(logicId), 0, bytes, 4, 4)
        Buffer.BlockCopy(nbytes, 0, bytes, 8, nbytes.Length)
        bytes

let deserialize (nbytes) (bytes:byte array) =
    let size = BitConverter.ToInt32(bytes, 0)
    let lid = BitConverter.ToInt32(bytes, 4)
    let note = Encoding.UTF8.GetString(bytes.[8..nbytes - 1])
    Msg(lid, note)

let sendWith (sok) (flags) (msg:TMsg) =
    let nbytes = NN.Send(sok, (serialize msg), flags)
    if nbytes < 0 then
        let errn = NN.Errno()
        let errm = NN.StrError(NN.Errno())
        Error(errn, errm)
    else
        msg

let send (sok) (msg:TMsg) =
    sendWith sok SendRecvFlags.NONE msg

let recvWith (sok) (flags) =
    let buff = Array.zeroCreate maxMessageSize
    let nbytes = NN.Recv(sok, buff, flags)
    if nbytes < 0 then
        let errn = NN.Errno()
        let errm = NN.StrError(NN.Errno())
        Error(errn, errm)
    else
        deserialize nbytes buff

let recv (sok) =
    recvWith sok SendRecvFlags.NONE
