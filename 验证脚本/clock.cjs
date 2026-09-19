const NativeDate = Date;
const fixed = Number(process.env.CACHE_FIXED_NOW);
global.Date = class extends NativeDate {
  constructor(...args) { super(...(args.length ? args : [fixed])); }
  static now() { return fixed; }
};
