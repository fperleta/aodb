-- extensions {{{
{-# LANGUAGE GADTs, RankNTypes #-}
-- }}}

-- exports {{{
module Database.AODB.Struct

    -- types.
    ( Struct()
    , Branch(..)

    -- front-end.
    , encode
    , decode

    -- simple structs.
    , word32
    , natural
    , byteString

    -- combinators.
    , struct
    , union
    , pairOf
    , tripleOf
    , listOf
    , dictOf

    ) where
-- }}}

-- imports {{{
import           Control.Monad
import           Data.Bits
import qualified Data.ByteString as B
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Builder as B
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Lazy as LB
import qualified Data.Map as M
import           Data.Map (Map)
import           Data.Monoid
import           Data.Word
-- }}}

-- types {{{

data Struct a = Struct
    { enc :: a -> Builder
    , dec :: ByteString -> Maybe (a, ByteString)
    }

data Branch k a = forall b. Branch
    { branchKey :: k
    , branchEnc :: a -> b
    , branchDec :: b -> Maybe a
    , branchStruct :: Struct b
    }

-- }}}

-- front-end {{{

encode :: Struct a -> a -> ByteString
encode s x = LB.toStrict . B.toLazyByteString $ enc s x

decode :: Struct a -> ByteString -> Maybe a
decode s bs = do
    (x, rest) <- dec s bs
    guard $ B.null rest
    return x

-- }}}

-- simple structs {{{

word32 :: Struct Word32
word32 = Struct B.word32BE $ \bs -> do
    guard $ B.length bs >= 4
    let (w, bs') = B.splitAt 4 bs
    let f = B.foldl' (\acc b -> acc * 256 + fromIntegral b) 0
    return (f w, bs')

natural :: Struct Word
natural = Struct enc' dec'
  where
    enc' n = case septets n of
        [] -> B.word8 0
        d:ds -> mconcat . map B.word8 . reverse $ d : map (.|. 0x80) ds
    septets = takeWhile (/= 0)
            . map (fromIntegral . (.&. 0x3F))
            . iterate (`shiftR` 7)

    dec' bs = let
        { go acc bs = do
            (b, bs') <- B.uncons bs
            let d = fromIntegral $ b .&. 0x3F
            let acc' = acc `shiftL` 7 .|. d
            if b .&. 0x80 == 0
                then return (acc', bs')
                else go acc' bs'
        } in go 0 bs

byteString :: Struct ByteString
byteString = Struct enc' dec'
  where
    enc' bs = enc natural (fromIntegral $ B.length bs) <> B.byteString bs

    dec' bs = do
        (len, bs') <- dec natural bs
        guard $ B.length bs' >= fromIntegral len
        let (x, bs'') = B.splitAt (fromIntegral len) bs'
        return (x, bs'')

-- }}}

-- combinators {{{

struct :: (a -> b) -> (b -> Maybe a) -> Struct b -> Struct a
struct f g s = Struct enc' dec'
  where
    enc' = enc s . f
    dec' bs = do
        (x, bs') <- dec s bs
        x' <- g x
        return (x', bs')

union :: (Eq k)
      => Struct k
      -> (a -> k)
      -> [Branch k a]
      -> Struct a
union key keyOf cs = Struct enc' dec'
  where
    enc' x = case keyOf x of
        k -> case [enc s (f x) | Branch k' f _ s <- cs, k' == k] of
            [b] -> enc key k <> b
            _ -> error "unionOf"
    dec' bs = do
        (k, bs') <- dec key bs
        case [ (x', bs'')
             | Branch k' _ g s <- cs, k' == k
             , let Just (x, bs'') = dec s bs'
             , let Just x' = g x
             ] of
            [(x, bs'')] -> Just (x, bs'')
            _ -> Nothing

pairOf :: Struct a -> Struct b -> Struct (a, b)
pairOf s1 s2 = Struct enc' dec'
  where
    enc' (x, y) = enc s1 x <> enc s2 y
    dec' bs = do
        (x, bs') <- dec s1 bs
        (y, bs'') <- dec s2 bs'
        return ((x, y), bs'')

tripleOf :: Struct a -> Struct b -> Struct c -> Struct (a, b, c)
tripleOf s1 s2 s3 = struct
    (\(x, y, z) -> (x, (y, z)))
    (\(x, (y, z)) -> Just (x, y, z))
    (pairOf s1 $ pairOf s2 s3)

listOf :: Struct a -> Struct [a]
listOf s = Struct enc' dec'
  where
    enc' xs = enc natural (fromIntegral $ length xs) <> mconcat (map (enc s) xs)
    dec' bs = do
        (len, bs') <- dec natural bs
        let go 0 bs = return ([], bs)
            go n bs = do
                (x, bs') <- dec s bs
                (xs, bs'') <- go (n - 1) bs'
                return (x:xs, bs'')
        go len bs'

dictOf :: (Ord k) => Struct k -> Struct v -> Struct (Map k v)
dictOf key val = Struct enc' dec'
  where
    s = listOf $ pairOf key val
    enc' = enc s . M.toAscList
    dec' bs = do
        (ps, bs') <- dec s bs
        return (M.fromList ps, bs')

-- }}}

-- vim:fdm=marker:
