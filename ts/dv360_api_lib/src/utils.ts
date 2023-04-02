/**
 * @license
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @fileoverview This file encapsulates all utility classes and methods used by
 * the underlying API client library.
 */

/**
 * Defines the logical grouping for filter expressions.
 */
export enum FilterGrouping {
  AND = ' AND ',
  OR = ' OR ',
}

/**
 * Defines the supported filter rule equality operators.
 */
export enum RuleOperator {
  EQ = '=',
  GTEQ = '>=',
  LTEQ = '<=',
}

/**
 * Represents a filter rule for use with instances of
 * {@link FilterExpression}.
 */
export class Rule {

  /**
   * Constructs an instance of Rule.
   *
   * @param field The field to apply the filter rule to
   * @param operator The equality operator to use
   * @param value The value to filter for
   */
  constructor(private readonly field: string, private readonly operator: RuleOperator, private readonly value: string | number) {
  }

  /**
   * Returns the field.
   *
   * @return The field
   */
  getField(): string {
    return this.field;
  }

  /**
   * Returns the operator.
   *
   * @return The operator
   */
  getOperator(): RuleOperator {
    return this.operator;
  }

  /**
   * Returns the value.
   *
   * @return The value
   */
  getValue(): string | number {
    return this.value;
  }

  /**
   * Returns a string representation of the Rule.
   *
   * @return The string representation of the Rule
   */
  toString(): string {
    const val = this.getValue();
    const valString = typeof val === `string` ? `"${val}"` : val;
    return `${this.getField()}${this.getOperator()}${valString}`;
  }
}

/**
 * Represents a FilterExpression that can be applied when listing API entities
 * to filter results accordingly.
 */
export class FilterExpression {
  /**
   * Constructs an instance of FilterExpression.
   *
   * @param rules The filter rules to apply
   * @param grouping Optional logical grouping for the given
   *     filter 'rules'. Defaults to AND
   */
  constructor(private readonly rules: Rule[], private readonly grouping: FilterGrouping = FilterGrouping.AND) {
  }

  /**
   * Applies the FilterExpression's grouping to its rules and returns the
   * API-ready value for the filter query string parameter. Returns an empty
   * string if no rules are present.
   *
   * @return The API-ready value for the 'filter' query string param,
   *     or an empty string if no rules are present
   */
  toApiQueryString(): string {
    const queryString = this.rules
      .map((rule) => rule.toString())
      .join(this.grouping);
    return encodeURIComponent(queryString);
  }
}

/**
 * Parameters that are allowed to be passed to `list` methods.
 */
export interface ListParams {
  filter?: FilterExpression | null;
  pageSize?: number;
  orderBy?: string;
}

/** Uility class for working with URIs. */
// tslint:disable-next-line:enforce-name-casing Legacy from JS migration
export const UriUtil = {
  /**
   * Modifies a url by either appending the 'key' and 'value' to the end of the
   * url if the 'key' was not present or replacing the value of the 'key' if it
   * existed. Multiple values for the same key will all be replaced by a single
   * key-value pair at the first seen key location. Assumes that all params have
   * already been URL encoded.
   *
   * @param url The url to modify
   * @param key The key to check if present
   * @param value The value to append / modify
   * @return The modified url
   */
  modifyUrlQueryString(url: string, key: string, value: string): string {
    let baseUrl: string;
    let queryString: string;
    let fragment: string;

    if (url.indexOf('?') !== -1) {
      [baseUrl, queryString] = url.split('?');
      fragment =
        queryString.indexOf('#') !== -1
          ? queryString.substring(queryString.indexOf('#'))
          : '';
      queryString = queryString.replace(fragment, '');
      const regExp = new RegExp(`(^|&)${key}=[^&]*`, 'g');
      const matches = queryString.match(regExp);

      if (matches) {
        let modified = false;

        matches.forEach((match) => {
          let replacement = '';

          if (!modified) {
            const val = match.substring(match.indexOf('=') + 1);
            replacement = match.replace(val, value);
            modified = true;
          }
          queryString = queryString.replace(match, replacement);
        });
      } else {
        const separator = queryString.length > 0 ? '&' : '';
        queryString += `${separator}${key}=${value}`;
      }
    } else {
      baseUrl = url;
      queryString = `${key}=${value}`;
      fragment = '';
    }
    return `${baseUrl}?${queryString}${fragment}`;
  },
};

/**
 * Class holding utility methods for working with objects. It can essentially be
 * considered an extension of the built in static methods provided by the
 * `Object` class.
 */
// tslint:disable-next-line:enforce-name-casing legacy from JS migration
export const ObjectUtil = {
  /**
   * Extends an object identified by 'original' with the values in 'extension'.
   * 'extension' will be returned if 'original' is null, otherwise 'original'
   * will get extended. Array values in 'extension' will be appended to existing
   * arrays in 'original', however all other objects in 'extension' will
   * override existing counterparts in 'original'. The plain JS type of
   * 'original' will be preserved (if it wasn't null or undefined - i.e. passing
   * an instance of a specific class will not be overrided, rather extended).
   *
   * @param original The original object to extend, which
   *     may be null
   * @param extension The value to use for extending
   * @return The extended object
   */
  extend<T extends object | null, E extends object>(
    original: T,
    extension: E
  ): T & E {
    if (original == null) {
      return {...extension} as T & E;
    }
    for (const key in extension) {
      if (extension.hasOwnProperty(key)) {
        const extensionValue = extension[key];
        const originalValue = (original as Record<string, string | string[]>)[
          key
        ];
        if (Array.isArray(extensionValue) && Array.isArray(originalValue)) {
          originalValue.push(...extensionValue);
        } else {
          (original as Record<string, E[keyof E]>)[key] = extension[key];
        }
      }
    }
    return original as T & E;
  },

  /**
   * Checks if the given object contains all of the given properties.
   *
   * @param obj The obj to check. Can be null or undefined
   * @param requiredProperties The required properties to
   *     check. Can only be empty if optionalProperties exist, otherwise false
   * will be returned
   * @param optionalProperties Optional properties to check.
   *     Object must contain at least one of these properties. Defaults to an
   *     empty array
   * @return True if the object contains all properties, false
   *     otherwise
   */
  hasOwnProperties(
    obj: unknown,
    requiredProperties: string[],
    optionalProperties: string[] = []
  ): boolean {
    const keys = ObjectUtil.isObject(obj)
      ? Object.keys(obj as {[key: string]: unknown})
      : [];
    return (
      keys.length > 0 &&
      (requiredProperties.length > 0 || optionalProperties.length > 0) &&
      requiredProperties.every((key) => keys.includes(key)) &&
      (optionalProperties.length === 0 ||
        optionalProperties.some((key) => keys.includes(key)))
    );
  },

  /**
   * Checks if the given object is indeed an object.
   *
   * @param obj The obj to check. Can be null or undefined
   * @return True if the object is an object, false otherwise
   */
  isObject(obj: unknown): boolean {
    return obj != null && obj instanceof Object && !Array.isArray(obj);
  },
};
