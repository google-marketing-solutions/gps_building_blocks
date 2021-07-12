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
 * @enum {string}
 */
const FilterGrouping = {
  AND: ' AND ',
  OR: ' OR ',
};

/**
 * Defines the supported filter rule equality operators.
 * @enum {string}
 */
const RuleOperator = {
  EQ: '=',
  GTEQ: '>=',
  LTEQ: '<=',
};

/**
 * Represents a filter rule for use with instances of
 * {@link FilterExpression}.
 */
class Rule {
  /**
   * Constructs an instance of Rule.
   *
   * @param {string} field The field to apply the filter rule to
   * @param {!RuleOperator} operator The equality operator to use
   * @param {string|number} value The value to filter for
   */
  constructor(field, operator, value) {
    /** @private @const {string} */
    this.field_ = field;

    /** @private @const {!RuleOperator} */
    this.operator_ = operator;

    /** @private @const {string|number} */
    this.value_ = value;
  }

  /**
   * Returns the field.
   *
   * @return {string} The field
   */
  getField() {
    return this.field_;
  }

  /**
   * Returns the operator.
   *
   * @return {!RuleOperator} The operator
   */
  getOperator() {
    return this.operator_;
  }

  /**
   * Returns the value.
   *
   * @return {string|number} The value
   */
  getValue() {
    return this.value_;
  }

  /**
   * Returns a string representation of the Rule.
   *
   * @return {string} The string representation of the Rule
   */
  toString() {
    const val = this.getValue();
    const valString = typeof val === `string` ? `"${val}"` : val;
    return `${this.getField()}${this.getOperator()}${valString}`;
  }
}

/**
 * Represents a FilterExpression that can be applied when listing API entities
 * to filter results accordingly.
 */
class FilterExpression {
  /**
   * Constructs an instance of FilterExpression.
   *
   * @param {!Array<!Rule>} rules The filter rules to apply
   * @param {!FilterGrouping=} grouping Optional logical grouping for the given
   *     filter 'rules'. Defaults to AND
   */
  constructor(rules, grouping = FilterGrouping.AND) {
    /** @private @const {!Array<!Rule>} */
    this.rules_ = rules;

    /** @private @const {!FilterGrouping} */
    this.grouping_ = grouping;
  }

  /**
   * Applies the FilterExpression's grouping to its rules and returns the
   * API-ready value for the filter query string parameter. Returns an empty
   * string if no rules are present.
   *
   * @return {string} The API-ready value for the 'filter' query string param,
   *     or an empty string if no rules are present
   */
  toApiQueryString() {
    const queryString =
        this.rules_.map((rule) => rule.toString()).join(this.grouping_);
    return encodeURIComponent(queryString);
  }
}

/** Uility class for working with URIs. */
class UriUtil {
  /**
   * Modifies a url by either appending the 'key' and 'value' to the end of the
   * url if the 'key' was not present or replacing the value of the 'key' if it
   * existed. Multiple values for the same key will all be replaced by a single
   * key-value pair at the first seen key location. Assumes that all params have
   * already been URL encoded.
   *
   * @param {string} url The url to modify
   * @param {string} key The key to check if present
   * @param {string} value The value to append / modify
   * @return {string} The modified url
   */
  static modifyUrlQueryString(url, key, value) {
    let baseUrl, queryString, fragment;

    if (url.indexOf('?') !== -1) {
      [baseUrl, queryString] = url.split('?');
      fragment = queryString.indexOf('#') !== -1 ?
          queryString.substring(queryString.indexOf('#')) :
          '';
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
  }
}

/**
 * Class holding utility methods for working with objects. It can essentially be
 * considered an extension of the built in static methods provided by the
 * `Object` class.
 */
class ObjectUtil {
  /**
   * Extends an object identified by 'original' with the values in 'extension'.
   * 'extension' will be returned if 'original' is null, otherwise 'original'
   * will get extended. Array values in 'extension' will be appended to existing
   * arrays in 'original', however all other objects in 'extension' will
   * override existing counterparts in 'original'. The plain JS type of
   * 'original' will be preserved (if it wasn't null or undefined - i.e. passing
   * an instance of a specific class will not be overrided, rather extended).
   *
   * @param {?Object<string, *>} original The original object to extend, which
   *     may be null
   * @param {!Object<string, *>} extension The value to use for extending
   * @return {!Object<string, *>} The extended object
   */
  static extend(original, extension) {
    if (original == null) {
      return extension;
    }
    for (const key in extension) {
      if (extension.hasOwnProperty(key)) {
        const extensionValue = extension[key];
        const originalValue = original[key];
        if (Array.isArray(extensionValue) && Array.isArray(originalValue)) {
          originalValue.push(...extensionValue);
        } else {
          original[key] = extension[key];
        }
      }
    }
    return original;
  }

  /**
   * Checks if the given object contains all of the given properties.
   *
   * @param {*} obj The obj to check. Can be null or undefined
   * @param {!Array<string>} requiredProperties The required properties to
   *     check. Can only be empty if optionalProperties exist, otherwise false
   * will be returned
   * @param {!Array<string>=} optionalProperties Optional properties to check.
   *     Object must contain at least one of these properties. Defaults to an
   *     empty array
   * @return {boolean} True if the object contains all properties, false
   *     otherwise
   */
  static hasOwnProperties(obj, requiredProperties, optionalProperties = []) {
    const keys = ObjectUtil.isObject(obj) ?
        Object.keys(/** @type {!Object<string, *>} */(obj)) :
        [];
    return keys.length > 0 &&
        (requiredProperties.length > 0 || optionalProperties.length > 0) &&
        requiredProperties.every((key) => keys.includes(key)) &&
        (optionalProperties.length === 0 ||
         optionalProperties.some((key) => keys.includes(key)));
  }

  /**
   * Checks if the given object is indeed an object.
   *
   * @param {*} obj The obj to check. Can be null or undefined
   * @return {boolean} True if the object is an object, false otherwise
   */
  static isObject(obj) {
    return obj != null && obj instanceof Object && !Array.isArray(obj);
  }
}
