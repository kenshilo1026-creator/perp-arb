//! Read-only metadata resolution. No guessed ticks, quantities or alias multipliers.
use crate::{shared::MarketKey, Venue};
use perp_arb_core::{
    decimal,
    instruments::{common_lot, Instrument, PriceRule, Units},
    Decimal,
};
use serde_json::Value;
use std::collections::HashMap;

type Map = HashMap<String, String>;
#[derive(Debug, Clone, Default)]
pub struct SymbolMappings {
    global: Map,
    venues: HashMap<String, Map>,
    native: HashMap<String, Map>,
}
fn mapping(value: Option<&Value>) -> Result<Map, String> {
    let mut out = Map::new();
    let Some(value) = value.filter(|v| !v.is_null()) else {
        return Ok(out);
    };
    for (key, value) in value
        .as_object()
        .ok_or("symbol mapping must be an object")?
    {
        let value = value
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or("mapping value must be a nonempty string")?;
        if key.is_empty()
            || out
                .insert(key.to_ascii_uppercase(), value.to_ascii_uppercase())
                .is_some()
        {
            return Err("empty/ambiguous symbol mapping".into());
        }
    }
    Ok(out)
}
fn venue_maps(value: Option<&Value>) -> Result<HashMap<String, Map>, String> {
    let mut out = HashMap::new();
    if let Some(value) = value.filter(|v| !v.is_null()) {
        for (venue, row) in value
            .as_object()
            .ok_or("venue mappings must be an object")?
        {
            if out
                .insert(venue.to_ascii_lowercase(), mapping(Some(row))?)
                .is_some()
            {
                return Err("ambiguous venue mapping".into());
            }
        }
    }
    Ok(out)
}
impl SymbolMappings {
    pub fn parse(value: &Value) -> Result<Self, String> {
        if !value.is_object() {
            return Err("mapping root must be an object".into());
        }
        Ok(Self {
            global: mapping(value.get("global"))?,
            venues: venue_maps(value.get("venues"))?,
            native: venue_maps(value.get("venue_symbols"))?,
        })
    }
    pub fn canonical(&self, symbol: &str, venue: &str) -> String {
        let key = symbol.to_ascii_uppercase();
        self.venues
            .get(&venue.to_ascii_lowercase())
            .and_then(|m| m.get(&key))
            .or_else(|| self.global.get(&key))
            .cloned()
            .unwrap_or(key)
    }
    pub fn native(&self, symbol: &str, venue: &str) -> String {
        let key = symbol.to_ascii_uppercase();
        self.native
            .get(&venue.to_ascii_lowercase())
            .and_then(|m| m.get(&key))
            .cloned()
            .unwrap_or(key)
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedInstrument {
    pub key: MarketKey,
    pub native_base: String,
    pub canonical_base: String,
    pub quote: String,
    pub settlement: String,
    /// Main-dex universe index, including preceding delisted entries.
    pub asset_id: Option<usize>,
    pub limit: Instrument,
    pub market: Option<Instrument>,
    units: Option<Units>,
}
impl ResolvedInstrument {
    pub fn external(
        key: MarketKey,
        base: String,
        quote: String,
        asset_id: Option<usize>,
        limit: Instrument,
    ) -> Self {
        Self {
            key,
            native_base: base.clone(),
            canonical_base: base,
            quote: quote.clone(),
            settlement: quote,
            asset_id,
            market: Some(limit.clone()),
            limit,
            units: Some(Units::new(Decimal::ONE).expect("one unit")),
        }
    }
    pub fn units(&self) -> Option<Units> {
        self.units
    }
    /// Caller must supply an externally verified unit scale. Merely normalizing
    /// a ticker (1000PEPE -> PEPE) is deliberately insufficient.
    pub fn with_verified_units(mut self, base_per_native: Decimal) -> Result<Self, String> {
        self.units = Some(Units::new(base_per_native).map_err(|e| e.to_string())?);
        Ok(self)
    }
    pub fn common_limit_base_lot(&self, other: &Self) -> Result<Decimal, String> {
        if self.key.venue() == other.key.venue() || self.canonical_base != other.canonical_base {
            return Err("different underlying or same venue".into());
        }
        let a = self.units.ok_or("unverified alias units")?;
        let b = other.units.ok_or("unverified alias units")?;
        let lots = [
            a.to_base(self.limit.lot()).map_err(|e| e.to_string())?,
            b.to_base(other.limit.lot()).map_err(|e| e.to_string())?,
        ];
        common_lot(&lots)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "missing lots".into())
    }
}
fn units(native: &str, canonical: &str) -> Option<Units> {
    (native.to_ascii_uppercase() == canonical)
        .then(|| Units::new(Decimal::ONE).expect("one is positive"))
}
fn string<'a>(row: &'a Value, key: &str) -> Result<&'a str, String> {
    row[key]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("missing string {key}"))
}
fn dec(row: &Value, key: &str) -> Result<Decimal, String> {
    decimal(string(row, key)?).map_err(|e| format!("invalid {key}: {e}"))
}
fn filters(row: &Value) -> Result<HashMap<&str, &Value>, String> {
    let mut out = HashMap::new();
    for filter in row["filters"].as_array().ok_or("missing filters")? {
        if out.insert(string(filter, "filterType")?, filter).is_some() {
            return Err("duplicate filter type".into());
        }
    }
    Ok(out)
}
fn quantity_rule(
    row: &Value,
    price: PriceRule,
    min_notional: Decimal,
) -> Result<Instrument, String> {
    Instrument::new(
        price,
        dec(row, "stepSize")?,
        dec(row, "minQty")?,
        Some(min_notional),
        Some(dec(row, "maxQty")?),
    )
    .map_err(|e| e.to_string())
}

pub fn resolve_aster(
    info: &Value,
    request: &str,
    maps: &SymbolMappings,
) -> Result<ResolvedInstrument, String> {
    let rows = info["symbols"].as_array().ok_or("missing Aster symbols")?;
    let requested = maps.native(request, "aster");
    let mut contracts = HashMap::new();
    for row in rows {
        let name = string(row, "symbol")?.to_ascii_uppercase();
        if contracts.insert(name, row).is_some() {
            return Err("ambiguous Aster contract".into());
        }
    }
    // Exact native requests do not silently fall through to another settlement.
    let row = if let Some(row) = contracts.get(&requested) {
        *row
    } else {
        ["USDT", "USDC", "USD"]
            .iter()
            .find_map(|suffix| {
                contracts
                    .get(&format!("{requested}{suffix}"))
                    .copied()
                    .filter(|r| r["status"] == "TRADING")
            })
            .ok_or("no trading Aster contract")?
    };
    if row["status"] != "TRADING" || row["contractType"] != "PERPETUAL" {
        return Err("Aster contract is not a trading perpetual".into());
    }
    let native = string(row, "symbol")?;
    let base = string(row, "baseAsset")?;
    let quote = string(row, "quoteAsset")?;
    let settlement = string(row, "marginAsset")?;
    // Explicit native names may use newer collateral denominations; retain
    // them verbatim. Base-symbol fallback deliberately keeps Python's order.
    if !["USDT", "USDC", "USD", "USD1", "U"].contains(&quote)
        || settlement != quote
        || !native.eq_ignore_ascii_case(&format!("{base}{quote}"))
    {
        return Err("unsupported or inconsistent Aster contract units".into());
    }
    let filters = filters(row)?;
    let price_filter = filters.get("PRICE_FILTER").ok_or("missing price filter")?;
    let price = PriceRule::Tick(dec(price_filter, "tickSize")?);
    let min_price = dec(price_filter, "minPrice")?;
    let max_price = dec(price_filter, "maxPrice")?;
    let notional = filters
        .get("MIN_NOTIONAL")
        .ok_or("missing minimum notional filter")?;
    let min = dec(
        notional,
        if notional.get("notional").is_some() {
            "notional"
        } else {
            "minNotional"
        },
    )?;
    let limit = quantity_rule(
        filters.get("LOT_SIZE").ok_or("missing lot filter")?,
        price,
        min,
    )?
    .with_price_bounds(min_price, max_price)
    .map_err(|e| e.to_string())?;
    // Do not guess MARKET_LOT_SIZE from precision or silently use limit rules.
    let market = filters
        .get("MARKET_LOT_SIZE")
        .map(|row| {
            quantity_rule(row, price, min)?
                .with_price_bounds(min_price, max_price)
                .map_err(|e| e.to_string())
        })
        .transpose()?;
    let canonical = maps.canonical(base, "aster");
    Ok(ResolvedInstrument {
        key: MarketKey::new(Venue::Aster, native)?,
        native_base: base.into(),
        units: units(base, &canonical),
        canonical_base: canonical,
        quote: quote.into(),
        settlement: settlement.into(),
        asset_id: None,
        limit,
        market,
    })
}

pub fn resolve_hyperliquid(
    meta: &Value,
    request: &str,
    maps: &SymbolMappings,
) -> Result<ResolvedInstrument, String> {
    if meta
        .get("collateralToken")
        .is_some_and(|v| v.as_u64() != Some(0))
    {
        return Err("only main-dex USDC collateral supported in this batch".into());
    }
    let requested = maps.native(request, "hyperliquid");
    if requested.contains(':') {
        return Err("HIP-3 resolution requires dex metadata; not implemented in this batch".into());
    }
    let rows = meta["universe"]
        .as_array()
        .ok_or("missing Hyperliquid universe")?;
    let matches: Vec<_> = rows
        .iter()
        .enumerate()
        .filter(|(_, row)| {
            row["name"]
                .as_str()
                .is_some_and(|s| s.eq_ignore_ascii_case(&requested))
        })
        .collect();
    if matches.len() != 1 {
        return Err("missing or ambiguous Hyperliquid coin".into());
    }
    let (index, row) = matches[0];
    if row
        .get("isDelisted")
        .is_some_and(|v| v.as_bool() != Some(false))
    {
        return Err("delisted or malformed Hyperliquid coin".into());
    }
    let name = string(row, "name")?;
    if name.contains(':') {
        return Err("HIP-3 requires separate asset ID resolution".into());
    }
    let sz = row["szDecimals"]
        .as_u64()
        .filter(|n| *n <= 6)
        .ok_or("invalid szDecimals")? as u32;
    let lot = Decimal::new(1, sz);
    let limit = Instrument::new(
        PriceRule::Hyperliquid { sz_decimals: sz },
        lot,
        lot,
        Some(Decimal::TEN),
        None,
    )
    .map_err(|e| e.to_string())?;
    let canonical = maps.canonical(name, "hyperliquid");
    Ok(ResolvedInstrument {
        key: MarketKey::new(Venue::Hyperliquid, name)?,
        native_base: name.into(),
        units: units(name, &canonical),
        canonical_base: canonical,
        quote: "USD".into(),
        settlement: "USDC".into(),
        asset_id: Some(index),
        market: Some(limit.clone()),
        limit,
    })
}
