-- extensions {{{
{-# LANGUAGE
        DeriveFunctor, FlexibleContexts, FlexibleInstances, FunctionalDependencies,
        GeneralizedNewtypeDeriving, MultiParamTypeClasses, OverloadedStrings,
        RankNTypes, UndecidableInstances
    #-}
-- }}}

-- exports {{{
module Database.AODB.Storage

    -- types.
    ( ChunkID(..)

    -- monad.
    , Storage()
    , MonadStorage(..)
    , StorageWhere(..)
    , runStorage

    -- operations.
    , lastChunk
    , storeChunk
    , recallChunk

    ) where
-- }}}

-- imports {{{
import           Control.Applicative
import           Control.Concurrent.MVar
import           Control.Exception
import           Control.Monad
import           Control.Monad.Reader
import           Data.Digest.CRC32 (crc32)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import           Data.Word (Word8, Word32)
import           Foreign.Marshal
import           Foreign.Ptr
import           Foreign.Storable
import           GHC.IO.Device hiding (close)
import           System.Posix
-- }}}

-- handles {{{

-- the format {{{ 
{-

A file is a sequence of chunks, with no padding or alignment.

Each chunk has the following layout:

    +0:u32le            self offset
    +4:u32le            content size
    +8:u32le            crc32 of content
    +12:u8[size]        content
    -4:u32le            self offset

There are 16 bytes of overhead per chunk.

-}
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
  deriving (Eq, Ord, Enum)

-- }}}

-- creating {{{

dbCreate :: FilePath -> FileMode -> IO (Handle db)
dbCreate fn fm = do
    -- open the file.
    let flags = defaultFileFlags { append = True, exclusive = True }
    fd <- openFd fn ReadWrite (Just fm) flags

    -- initialize the handle state.
    mhs <- newMVar $ HS
        { hsFd = fd
        , hsLast = ChunkID 0
        }

    let h = Handle
            { handlePath = fn
            , handleState = mhs
            }

    dbAppend h "aodb"

    return h

-- }}}

-- opening {{{

dbOpen :: FilePath -> IO (Handle db)
dbOpen fn = do
    -- open the file.
    let flags = defaultFileFlags { append = True }
    fd <- openFd fn ReadWrite Nothing flags

    -- determine the last chunk.
    blah <- fdSeek fd SeekFromEnd (-4)
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

dbClose :: Handle db -> IO ()
dbClose h = do
    hs <- takeMVar $ handleState h
    closeFd $ hsFd hs
    putMVar (handleState h) hs

-- }}}

-- appending {{{

dbAppend :: Handle db -> ByteString -> IO (ChunkID db)
dbAppend h bs = do
    hs <- takeMVar $ handleState h

    -- prefix part.
    offs <- fromIntegral `fmap` fdSeek (hsFd hs) SeekFromEnd 0
    allocaBytes 12 $ \buf -> do
        let _ = buf :: Ptr Word32
        pokeElemOff buf 0 offs
        pokeElemOff buf 1 . fromIntegral $ B.length bs
        pokeElemOff buf 2 $ crc32 bs
        fdWriteBuf (hsFd hs) (castPtr buf) 12

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

dbRead :: Handle db -> ChunkID db -> IO ByteString
dbRead h (ChunkID offs) = do
    hs <- takeMVar $ handleState h

    fdSeek (hsFd hs) AbsoluteSeek $ fromIntegral offs
    (len, checksum) <- allocaBytes 12 $ \buf -> do
        let _ = buf :: Ptr Word32
        fdReadBuf (hsFd hs) (castPtr buf) 12
        offs' <- peekElemOff buf 0
        len <- fromIntegral `fmap` peekElemOff buf 1
        checksum <- peekElemOff buf 2
        assert (offs == offs') $ return (len, checksum)

    bs <- allocaBytes len $ \buf -> do
        let _ = buf :: Ptr Word8
        fdReadBuf (hsFd hs) (castPtr buf) $ fromIntegral len
        B.packCStringLen (castPtr buf, len)

    putMVar (handleState h) hs

    assert (checksum == crc32 bs) $ return bs

-- }}}

-- }}}

-- the storage monad {{{

newtype Storage db a = Storage { unStorage :: ReaderT (Handle db) IO a }
  deriving (Functor, Applicative, Monad, MonadIO)

class (Monad m, MonadIO m) => MonadStorage db m | m -> db where
    askHandle :: m (Handle db)

instance MonadStorage db (Storage db) where
    askHandle = Storage ask

instance (MonadStorage db m, Monad (t m), MonadIO (t m), MonadTrans t) => MonadStorage db (t m) where
    askHandle = lift $ askHandle

-- running {{{

data StorageWhere
    = Existing FilePath
    | NewFile FilePath FileMode

runStorage :: StorageWhere -> (forall db. Storage db a) -> IO a
runStorage wh action = do
    h <- case wh of
        Existing fn -> dbOpen fn
        NewFile fn fm -> dbCreate fn fm
    runReaderT (unStorage action) h `finally` dbClose h

-- }}}

-- operations {{{

lastChunk :: (MonadStorage db m) => m (ChunkID db)
lastChunk = do
    mv <- handleState `liftM` askHandle
    hs <- liftIO $ takeMVar mv
    liftIO $ putMVar mv hs
    return $ hsLast hs

storeChunk :: (MonadStorage db m) => ByteString -> m (ChunkID db)
storeChunk bs = do
    h <- askHandle
    liftIO $ dbAppend h bs

recallChunk :: (MonadStorage db m) => ChunkID db -> m ByteString
recallChunk ch = do
    h <- askHandle
    liftIO $ dbRead h ch

-- }}}

-- }}}

-- vim:fdm=marker:
