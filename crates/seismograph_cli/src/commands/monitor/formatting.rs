// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Shared formatting for monitor models, reports, and terminal views.

use jiff::fmt::friendly::{FractionalUnit, Spacing, SpanPrinter};
use jiff::{SignedDuration, SignedDurationRound, Unit};

pub(super) fn format_count(value: u64) -> String {
    let digits = value.to_string();
    let mut formatted = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index != 0 && (digits.len() - index).is_multiple_of(3) {
            formatted.push(',');
        }
        formatted.push(digit);
    }
    formatted
}

pub(super) fn format_bytes(bytes: u64) -> String {
    const KIBIBYTE: u64 = 1024;
    const MEBIBYTE: u64 = KIBIBYTE * 1024;
    const GIBIBYTE: u64 = MEBIBYTE * 1024;
    if bytes >= GIBIBYTE {
        format_decimal(bytes, GIBIBYTE, "GiB", true)
    } else if bytes >= MEBIBYTE {
        format_decimal(bytes, MEBIBYTE, "MiB", true)
    } else if bytes >= KIBIBYTE {
        format_decimal(bytes, KIBIBYTE, "KiB", true)
    } else {
        format!("{bytes} B")
    }
}

pub(super) fn format_runtime_duration(nanos: u64) -> String {
    format_duration(nanos, Spacing::None, Some(2))
}

pub(super) fn format_prose_duration(nanos: u64) -> String {
    format_duration(nanos, Spacing::BetweenUnits, Some(2))
}

pub(super) fn format_histogram_duration(nanos: u64) -> String {
    format_duration(nanos, Spacing::None, Some(0))
}

fn format_duration(nanos: u64, spacing: Spacing, precision: Option<u8>) -> String {
    let mut duration = SignedDuration::from_nanos_i128(i128::from(nanos));
    if precision == Some(2) {
        let smallest = match nanos {
            0..1_000 => None,
            1_000..1_000_000 => Some(Unit::Nanosecond),
            1_000_000..1_000_000_000 => Some(Unit::Microsecond),
            _ => Some(Unit::Millisecond),
        };
        if let Some(smallest) = smallest {
            duration = duration
                .round(SignedDurationRound::new().smallest(smallest).increment(10))
                .expect("fixed decimal-duration rounding units and increment are valid");
        }
    }
    let rounded_nanos = u64::try_from(duration.as_nanos()).expect("a nonnegative u64 nanosecond duration remains nonnegative");
    let (fractional, zero_unit) = match rounded_nanos {
        0..1_000 => (None, Unit::Nanosecond),
        1_000..1_000_000 => (Some(FractionalUnit::Microsecond), Unit::Microsecond),
        1_000_000..1_000_000_000 => (Some(FractionalUnit::Millisecond), Unit::Millisecond),
        _ => (Some(FractionalUnit::Second), Unit::Second),
    };
    SpanPrinter::new()
        .spacing(spacing)
        .fractional(fractional)
        .zero_unit(zero_unit)
        .precision(precision)
        .duration_to_string(&duration)
}

fn format_decimal(value: u64, unit: u64, suffix: &str, space: bool) -> String {
    let whole = value / unit;
    let hundredths = value % unit * 100 / unit;
    let separator = if space { " " } else { "" };
    format!("{whole}.{hundredths:02}{separator}{suffix}")
}
