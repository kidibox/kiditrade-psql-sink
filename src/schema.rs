table! {
    bars (symbol, time, size) {
        symbol -> Varchar,
        time -> Timestamptz,
        size -> Int4,
        open -> Numeric,
        high -> Numeric,
        low -> Numeric,
        close -> Numeric,
        wap -> Numeric,
        volume -> Int8,
        trades -> Int8,
    }
}
