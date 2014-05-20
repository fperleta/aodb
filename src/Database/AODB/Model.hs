-- extensions {{{
{-# LANGUAGE
        DataKinds, DeriveFunctor, FlexibleContexts, FlexibleInstances,
        FunctionalDependencies, GADTs, GeneralizedNewtypeDeriving,
        MultiParamTypeClasses, KindSignatures, OverloadedStrings,
        RankNTypes, TypeFamilies, UndecidableInstances
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
import           Control.Applicative
import           Control.Monad
import           Control.Monad.State.Strict
import qualified Data.ByteString as B
import           Data.ByteString (ByteString)
import qualified Data.IntMap.Strict as IM
import           Data.IntMap.Strict (IntMap)
import           Data.List (isPrefixOf)
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
    , commitSteps :: [CommitStep db] -- reverse order
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

-- the monad {{{

newtype Model db a = Model { unModel :: StateT (Head db) (Storage db) a }
  deriving (Functor, Applicative, Monad, MonadIO)

instance MonadStorage db (Model db) where
    askHandle = Model askHandle

class (MonadStorage db m) => MonadModel db m | m -> db where
    modelState :: (Head db -> (a, Head db)) -> m a
    modelGets :: (Head db -> a) -> m a
    modelGets f = modelState $ \h -> (f h, h)

instance MonadModel db (Model db) where
    modelState = Model . state

instance (MonadModel db m, Monad (t m), MonadStorage db (t m), MonadTrans t) => MonadModel db (t m) where
    modelState = lift . modelState

-- head {{{

data Head db = Head
    { headTip :: Maybe (Ref db Commit)
    , headPending :: [CommitStep db]
    , headBlobs :: !(IntMap ByteString)
    , headTrees :: !(IntMap (TreeObj db))
    , headCommits :: !(IntMap (CommitObj db))
    }

headInit :: Ref db Commit -> Head db
headInit tip = Head
    { headTip = case tip of
        Ref _ (ChunkID 0) -> Nothing
        _ -> Just tip
    , headPending = []
    , headBlobs = IM.empty
    , headTrees = IM.empty
    , headCommits = IM.empty
    }

-- }}}

-- running {{{

runModel :: StorageWhere -> (forall db. Model db a) -> IO a
runModel wh action = runStorage wh $ do
    hd <- (headInit . Ref CommitTag) <$> lastChunk
    let action' = do res <- action; commit; return res
    (x, _) <- runStateT (unModel action') hd
    return x

-- }}}

-- dereferencing {{{

derefBlob :: (MonadModel db m) => Ref db Blob -> m ByteString
derefBlob (Ref _ ch) = do
    blobs <- modelGets headBlobs
    let k = fromEnum ch
    case IM.lookup k blobs of
        Just bs -> return bs
        Nothing -> do
            bs <- recallChunk ch
            modelState $ \h ->
                (bs, h { headBlobs = IM.insert k bs blobs })

derefTree :: (MonadModel db m) => Ref db Tree -> m (TreeObj db)
derefTree r@(Ref _ ch) = do
    trees <- modelGets headTrees
    let k = fromEnum ch
    case IM.lookup k trees of
        Just x -> return x
        Nothing -> do
            x <- readRef r
            modelState $ \h ->
                (x, h { headTrees = IM.insert k x trees })

derefCommit :: (MonadModel db m) => Ref db Commit -> m (CommitObj db)
derefCommit r@(Ref _ ch) = do
    commits <- modelGets headCommits
    let k = fromEnum ch
    case IM.lookup k commits of
        Just x -> return x
        Nothing -> do
            x <- readRef r
            modelState $ \h ->
                (x, h { headCommits = IM.insert k x commits })

-- }}}

-- path resolution {{{

resolvePath :: (IsObjType ty, MonadModel db m)
            => ObjTag ty -> TreePath -> m (Maybe (Ref db ty))
resolvePath tag path = do
    tip <- modelGets headTip
    pending <- modelGets headPending
    go tip pending
  where
    past Nothing = return Nothing
    past (Just cref) = do
        CommitObj prev steps <- derefCommit cref
        go prev steps

    go prev [] = past prev
    go prev (s:ss) = case tag of
        BlobTag -> case s of
            CommitBlob path' ref
                | path' == path -> return $ Just ref
            CommitTree path' ref
                | path' `isPrefixOf` path && path' /= path
                    -> blob ref $ drop (length path') path
            _ -> go prev ss
        TreeTag -> case s of
            CommitTree path' ref
                | path' `isPrefixOf` path -> tree ref $ drop (length path') path
            _ -> go prev ss
        _ -> error "resolvePath"

    blob ref [] = return Nothing
    blob ref (n:ns) = do
        TreeObj bs ts <- derefTree ref
        case ns of
            [] -> return $ M.lookup n bs
            _ -> case M.lookup n ts of
                Just ref' -> blob ref' ns
                Nothing -> return Nothing

    tree ref [] = return $ Just ref
    tree ref (n:ns) = do
        TreeObj _ ts <- derefTree ref
        case M.lookup n ts of
            Just ref' -> tree ref' ns
            Nothing -> return Nothing

-- }}}

-- path mutation {{{

linkPath :: (IsObjType ty, MonadModel db m) => TreePath -> Ref db ty -> m (Ref db ty)
linkPath path ref = modelState $ \h -> let
    { step = case ref of
        Ref BlobTag _ -> CommitBlob path ref
        Ref TreeTag _ -> CommitTree path ref
        _ -> error "linkPath"
    } in (ref, h { headPending = step : headPending h })

writeBlob :: (MonadModel db m) => TreePath -> ByteString -> m (Ref db Blob)
writeBlob path bs = linkPath path =<< newRef BlobTag (BlobObj bs)

writeTree :: (MonadModel db m) => TreePath -> TreeObj db -> m (Ref db Tree)
writeTree path tree = linkPath path =<< newRef TreeTag tree

-- }}}

-- commiting {{{

commit :: (MonadModel db m) => m (Ref db Commit)
commit = do
    tip <- modelGets headTip
    pending <- modelGets headPending
    case pending of
        [] -> case tip of
            Just ref -> return ref
            Nothing -> do
                tip' <- newRef CommitTag $ CommitObj Nothing []
                modelState $ \h -> (tip', h { headTip = Just tip' })
        _ -> do
            tip' <- newRef CommitTag $ CommitObj tip pending
            modelState $ \h -> (tip', h { headTip = Just tip', headPending = [] })

-- }}}

-- }}}

-- vim:fdm=marker:
