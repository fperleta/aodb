-- extensions {{{
{-# LANGUAGE
        DataKinds, GADTs, GeneralizedNewtypeDeriving, KindSignatures,
        OverloadedStrings, TypeFamilies
    #-}
-- }}}

-- exports {{{
module Database.AODB.Model

    -- object types.
    ( ObjType(..)
    , ObjTag(..)
    , IsObjType

    -- references.
    , Ref()
    , refS
    , nullRefS
    , newRef
    , readRef

    -- blobs.
    , BlobObj(..)

    -- trees.
    , TreeName(..)
    , treeNameS
    , TreePath
    , treePathS
    , TreeObj(..)
    , treeS

    -- commits.
    , CommitObj(..)
    , CommitStep(..)

    ) where
-- }}}

-- imports {{{
import qualified Data.ByteString as B
import           Data.ByteString (ByteString)
import qualified Data.Map as M
import           Data.Map (Map)
import           Data.String

import           Database.AODB.Storage
import qualified Database.AODB.Struct as S
import           Database.AODB.Struct (Struct)
-- }}}

-- object types {{{

data ObjType
    = Blob
    | Tree
    | Commit

data ObjTag :: ObjType -> * where
    BlobTag :: ObjTag Blob
    TreeTag :: ObjTag Tree
    CommitTag :: ObjTag Commit

class IsObjType (ty :: ObjType) where
    type ObjRepr ty :: * -> *
    objTag :: f ty -> ObjTag ty
    objEncode :: ObjTag ty -> ObjRepr ty db -> ByteString
    objDecode :: ObjTag ty -> ByteString -> Maybe (ObjRepr ty db)

-- }}}

-- references {{{

data Ref :: * -> ObjType -> * where
    Ref :: !(ObjTag ty)
        -> {-# UNPACK #-} !(ChunkID db)
        -> Ref db ty

refS :: (IsObjType ty) => ObjTag ty -> Struct (Ref db ty)
refS tag = S.struct
    (\(Ref _ ch) -> fromIntegral $ unChunkID ch)
    (Just . Ref tag . ChunkID . fromIntegral)
    S.natural

nullRefS :: (IsObjType ty) => ObjTag ty -> Struct (Maybe (Ref db ty))
nullRefS tag = S.struct
    (\x -> case x of Just (Ref _ ch) -> fromIntegral $ unChunkID ch; Nothing -> 0)
    (\n -> Just $ case n of 0 -> Nothing; _ -> Just . Ref tag . ChunkID $ fromIntegral n)
    S.natural

newRef :: (IsObjType ty, MonadStorage db m) => ObjTag ty -> ObjRepr ty db -> m (Ref db ty)
newRef tag x = do
    ch <- storeChunk $ objEncode tag x
    return $ Ref tag ch

readRef :: (IsObjType ty, MonadStorage db m) => Ref db ty -> m (ObjRepr ty db)
readRef (Ref tag ch) = do
    bs <- recallChunk ch
    case objDecode tag bs of
        Just x -> return x
        Nothing -> error "readObj"

-- }}}

-- blobs {{{

newtype BlobObj db = BlobObj { unBlobObj :: ByteString }

instance IsObjType Blob where
    type ObjRepr Blob = BlobObj
    objTag _ = BlobTag
    objEncode _ = unBlobObj
    objDecode _ = Just . BlobObj

-- }}}

-- trees {{{

newtype TreeName = TreeName { unTreeName :: ByteString }
  deriving (Show, Eq, Ord, IsString)

treeNameS = S.struct unTreeName (Just . TreeName) S.byteString

type TreePath = [TreeName]

treePathS = S.listOf treeNameS

data TreeObj db = TreeObj
    { treeBlobs :: Map TreeName (Ref db Blob)
    , treeTrees :: Map TreeName (Ref db Tree)
    }

instance IsObjType Tree where
    type ObjRepr Tree = TreeObj
    objTag _ = TreeTag
    objEncode _ = S.encode treeS
    objDecode _ = S.decode treeS

treeS = S.struct
    (\(TreeObj bs ts) -> (bs, ts))
    (Just . uncurry TreeObj)
    (S.pairOf (S.dictOf treeNameS $ refS BlobTag)
              (S.dictOf treeNameS $ refS TreeTag))

-- }}}

-- commits {{{

data CommitObj db = CommitObj
    { commitPrev :: Maybe (Ref db Commit)
    , commitSteps :: [CommitStep db]
    }

data CommitStep db
    = CommitBlob TreePath (Ref db Blob)
    | CommitTree TreePath (Ref db Tree)

instance IsObjType Commit where
    type ObjRepr Commit = CommitObj
    objTag _ = CommitTag
    objEncode _ = S.encode commitS
    objDecode _ = S.decode commitS

commitS = S.struct
    (\(CommitObj prev steps) -> (prev, steps))
    (\(prev, steps) -> Just $ CommitObj prev steps)
    (S.pairOf (nullRefS CommitTag) (S.listOf commitStepS))

commitStepS = S.union S.byteString keyOf
    [ S.Branch "blob"
        (\(CommitBlob p r) -> (p, r))
        (Just . uncurry CommitBlob)
        (S.pairOf treePathS $ refS BlobTag)
    , S.Branch "tree"
        (\(CommitTree p r) -> (p, r))
        (Just . uncurry CommitTree)
        (S.pairOf treePathS $ refS TreeTag)
    ]
  where
    keyOf b = case b of
        CommitBlob {} -> "blob"
        CommitTree {} -> "tree"

-- }}}

-- vim:fdm=marker:
