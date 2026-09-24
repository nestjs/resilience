/**
 * @internal An invalid option, named by its path: `maxConcurrent` where a
 * policy is created, `presets.shipco.bulkhead.maxConcurrent` at bootstrap.
 */
export class OptionError extends TypeError {
  constructor(
    readonly option: string,
    readonly problem: string,
  ) {
    super(option ? `${option}: ${problem}` : problem);
    this.name = 'TypeError';
  }
}
