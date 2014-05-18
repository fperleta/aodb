-- exports {{{
module Database.AODB.Handle

    -- types.
    ( Handle()
    , ChunkID()

    -- operations.
    , create
    , open
    , close
    , appendBS
    , readBS

    ) where
-- }}}

-- imports {{{
import           Control.Concurrent.MVar
import           Control.Exception
import           Data.Digest.CRC32 (crc32)
import           Data.Word (Word8, Word32)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import           Foreign.Marshal
import           Foreign.Ptr
import           Foreign.Storable
import           GHC.IO.Device hiding (close)
import           System.Posix
-- }}}

-- types {{{

data Handle db = Handle
    { handlePath :: FilePath
    , handleState :: !(MVar (HandleState db))
    }

data HandleState db = HS
    { hsFd :: Fd
    , hsLast :: ChunkID db
    }

newtype ChunkID db = ChunkID { unChunkID :: Word32 }

-- }}}

-- the format {{{ 
{-

The file is a sequence of chunks, with no padding or alignment.

Each chunk has the following layout:

    +0:u32le            self offset
    +4:u32le            previous chunk offset
    +8:u32le            content size
    +12:u32le           crc32 of content
    +16:u8[size]        content
    -4:u32le            self offset

There are 20 bytes of overhead per chunk.

-}
-- }}}

-- creating {{{

create :: FilePath -> FileMode -> IO (Handle db)
create fn fm = do
    -- open the file.
    let flags = defaultFileFlags { append = True, exclusive = True }
    fd <- openFd fn ReadWrite (Just fm) flags

    -- initialize the handle state.
    mhs <- newMVar $ HS
        { hsFd = fd
        , hsLast = ChunkID maxBound
        }

    return $ Handle
        { handlePath = fn
        , handleState = mhs
        }

-- }}}

-- opening {{{

open :: FilePath -> IO (Handle db)
open fn = do
    -- open the file.
    let flags = defaultFileFlags { append = True }
    fd <- openFd fn ReadWrite Nothing flags

    -- determine the last chunk.
    fdSeek fd SeekFromEnd 4
    last <- alloca $ \buf -> do
        let _ = buf :: Ptr Word32
        fdReadBuf fd (castPtr buf) 4
        peekElemOff buf 0

    -- initialize the handle state.
    mhs <- newMVar $ HS
        { hsFd = fd
        , hsLast = ChunkID last
        }

    return $ Handle
        { handlePath = fn
        , handleState = mhs
        }

-- }}}

-- closing {{{

close :: Handle db -> IO ()
close h = do
    hs <- takeMVar $ handleState h
    closeFd $ hsFd hs
    putMVar (handleState h) hs

-- }}}

-- appending {{{

appendBS :: Handle db -> ByteString -> IO (ChunkID db)
appendBS h bs = do
    hs <- takeMVar $ handleState h

    -- prefix part.
    offs <- fromIntegral `fmap` fdSeek (hsFd hs) SeekFromEnd 0
    allocaBytes 16 $ \buf -> do
        let _ = buf :: Ptr Word32
        pokeElemOff buf 0 offs
        pokeElemOff buf 1 . unChunkID $ hsLast hs
        pokeElemOff buf 2 . fromIntegral $ B.length bs
        pokeElemOff buf 3 $ crc32 bs
        fdWriteBuf (hsFd hs) (castPtr buf) 16

    -- the content.
    B.useAsCStringLen bs $ \(buf, len) -> do
        fdWriteBuf (hsFd hs) (castPtr buf) $ fromIntegral len

    -- suffix part.
    alloca $ \buf -> do
        let _ = buf :: Ptr Word32
        pokeElemOff buf 0 offs
        fdWriteBuf (hsFd hs) (castPtr buf) 4

    putMVar (handleState h) hs

    return . ChunkID $ fromIntegral offs

-- }}}

-- reading {{{

readBS :: Handle db -> ChunkID db -> IO ByteString
readBS h (ChunkID offs) = do
    hs <- takeMVar $ handleState h

    fdSeek (hsFd hs) AbsoluteSeek $ fromIntegral offs
    (len, checksum) <- allocaBytes 16 $ \buf -> do
        let _ = buf :: Ptr Word32
        fdReadBuf (hsFd hs) (castPtr buf) 16
        offs' <- peekElemOff buf 0
        len <- fromIntegral `fmap` peekElemOff buf 2
        checksum <- peekElemOff buf 3
        assert (offs == offs') $ return (len, checksum)

    bs <- allocaBytes len $ \buf -> do
        let _ = buf :: Ptr Word8
        fdReadBuf (hsFd hs) (castPtr buf) $ fromIntegral len
        B.packCStringLen (castPtr buf, len)

    putMVar (handleState h) hs

    assert (checksum == crc32 bs) $ return bs


-- }}}

-- vim:fdm=marker:
