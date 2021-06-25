'use strict';

export const toOid = function(oid: any): any {
  return oid
}

export const parseTimeToSeconds = function (value?: number|string, defaultValue?: number): number {
  if (!value && value !== 0) return defaultValue || 0;
  return Number(value);
}

export const sleep = function(s: number) {
  const ms = s * 1000
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
} 