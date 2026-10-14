/* Site calendar arithmetic uses IANA rules; UTC timestamps remain unchanged. */
const SITE_TIME_ZONE = document.querySelector('meta[name="wattimize-timezone"]')?.content || "Australia/Sydney";

class SiteDate extends WattimizeTZ.TZDate {
  constructor(...args) {
    // Offset-free input fields and weather timestamps are site wall times.
    const wall = args.length === 1 && typeof args[0] === "string"
      ? /^(\d{4})-(\d{2})-(\d{2})(?:T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?)?$/.exec(args[0])
      : null;
    if (wall) {
      args = [+wall[1], +wall[2] - 1, +wall[3], +(wall[4] || 0), +(wall[5] || 0), +(wall[6] || 0), +(wall[7] || "0").padEnd(3, "0")];
    }
    super(...(args.length ? args : [Date.now()]), SITE_TIME_ZONE);
  }

  // API ranges and persisted timestamps must retain native UTC serialization.
  toISOString() {
    return Date.prototype.toISOString.call(this);
  }
}
