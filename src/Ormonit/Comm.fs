module internal Comm

open System
open System.Text
open Cilnn

let maxMessageSize = 512

type Error = int32 * string
/// Transmission message
type TMsg = 
    | Msg of string * string
    | Error of Error
    static member Empty = Msg(String.Empty, String.Empty)

let Emptym = TMsg.Empty

type Envelop = 
    { msg : TMsg
      timeStamp : DateTimeOffset }
    static member Empty = 
        { msg = TMsg.Empty
          timeStamp = DateTimeOffset.MinValue }

let EmptyEnvelop = Envelop.Empty

let serialize (msg : TMsg) : byte array = 
    match msg with
    | Error(errn, errm) -> [||]
    | Msg(ckey, note) -> 
        let ksize = Encoding.UTF8.GetByteCount(ckey)
        let nsize = Encoding.UTF8.GetByteCount(note)
        let msize = ksize + nsize + 8 // [4] + [2]ckey + [2]note
        let bytes : byte array = Array.zeroCreate msize
        let kbytes = Encoding.UTF8.GetBytes(ckey)
        let nbytes = Encoding.UTF8.GetBytes(note)
        Buffer.BlockCopy(BitConverter.GetBytes(msize), 0, bytes, 0, 4)
        Buffer.BlockCopy(BitConverter.GetBytes(int16 (ksize)), 0, bytes, 4, 2)
        Buffer.BlockCopy(kbytes, 0, bytes, 6, kbytes.Length)
        let koffset = 6 + kbytes.Length
        Buffer.BlockCopy(BitConverter.GetBytes(int16 (nsize)), 0, bytes, koffset, 2)
        Buffer.BlockCopy(nbytes, 0, bytes, koffset + 2, nbytes.Length)
        bytes

let deserialize (nbytes) (bytes : byte array) = 
    let msize = BitConverter.ToInt32(bytes, 0)
    //TODO: Check maxMessageSize
    let ksize = int32 (BitConverter.ToInt16(bytes, 4))
    
    let kendi = 
        if ksize = 0 then 6
        else (6 + ksize - 1)
    
    let ckey = 
        if ksize = 0 then ""
        else Encoding.UTF8.GetString(bytes.[6..kendi])
    
    let nbegin = 
        if ksize = 0 then kendi
        else kendi + 1
    
    let nsize = int32 (BitConverter.ToInt16(bytes, nbegin))
    let note = Encoding.UTF8.GetString(bytes.[nbegin + 2..nbytes - 1])
    //missing c in "lose"
    Msg(ckey, note)

let sendWith (sok) (flags) (msg : TMsg) = 
    let nbytes = Nn.Send(sok, (serialize msg), flags)
    if nbytes < 0 then 
        let errn = Nn.Errno()
        let errm = Nn.StrError(Nn.Errno())
        Error(errn, errm)
    else msg

let send (sok) (msg : TMsg) = sendWith sok SendRecvFlags.NONE msg

let recvWith (sok) (flags) = 
    let buff = Array.zeroCreate maxMessageSize
    let nbytes = Nn.Recv(sok, buff, flags)
    if nbytes < 0 then 
        let errn = Nn.Errno()
        let errm = Nn.StrError(Nn.Errno())
        Error(errn, errm)
    else deserialize nbytes buff

let recv (sok) = recvWith sok SendRecvFlags.NONE

