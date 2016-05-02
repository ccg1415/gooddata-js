// Copyright (C) 2007-2014, GoodData(R) Corporation. All rights reserved.
import $ from 'jquery';
import md5 from 'md5';

import {
    ajax,
    post
} from './xhr';

import Rules from './utils/rules';

import invariant from 'invariant';
import {
    compact,
    filter,
    first,
    find,
    map,
    every,
    get,
    isEmpty,
    negate,
    last,
    assign,
    partial,
    flatten,
} from 'lodash';

const notEmpty = negate(isEmpty);

/**
 * Module for execution on experimental execution resource
 *
 * @class execution
 * @module execution
 */

/**
 * For the given projectId it returns table structure with the given
 * elements in column headers.
 *
 * @method getData
 * @param {String} projectId - GD project identifier
 * @param {Array} elements - An array of attribute or metric identifiers.
 * @param {Object} executionConfiguration - Execution configuration - can contain for example
 *                 property "filters" containing execution context filters
 *                 property "where" containing query-like filters
 *                 property "orderBy" contains array of sorted properties to order in form
 *                      [{column: 'identifier', direction: 'asc|desc'}]
 *
 * @return {Object} Structure with `headers` and `rawData` keys filled with values from execution.
 */
export function getData(projectId, elements, executionConfiguration = {}) {
    const executedReport = {
        isLoaded: false
    };

    // Create request and result structures
    const request = {
        execution: {
            columns: elements
        }
    };
    // enrich configuration with supported properties such as
    // where clause with query-like filters or execution context filters
    ['filters', 'where', 'orderBy', 'definitions'].forEach(property => {
        if (executionConfiguration[property]) {
            request.execution[property] = executionConfiguration[property];
        }
    });

    /* eslint-disable new-cap */
    const d = $.Deferred();
    /* eslint-enable new-cap */

    // Execute request
    post('/gdc/internal/projects/' + projectId + '/experimental/executions', {
        data: JSON.stringify(request)
    }, d.reject).then(function resolveSimpleExecution(result) {
        // TODO: when executionResult.headers will be globaly available columns map code should be removed
        if (result.executionResult.headers) {
            executedReport.headers = result.executionResult.headers;
        } else {
            // Populate result's header section if is not available
            executedReport.headers = result.executionResult.columns.map(function mapColsToHeaders(col) {
                if (col.attributeDisplayForm) {
                    return {
                        type: 'attrLabel',
                        id: col.attributeDisplayForm.meta.identifier,
                        uri: col.attributeDisplayForm.meta.uri,
                        title: col.attributeDisplayForm.meta.title
                    };
                }
                return {
                    type: 'metric',
                    id: col.metric.meta.identifier,
                    uri: col.metric.meta.uri,
                    title: col.metric.meta.title,
                    format: col.metric.content.format
                };
            });
        }
        // Start polling on url returned in the executionResult for tabularData
        return ajax(result.executionResult.tabularDataResult);
    }, d.reject).then(function resolveDataResultPolling(result, message, response) {
        // After the retrieving computed tabularData, resolve the promise
        executedReport.rawData = (result && result.tabularDataResult) ? result.tabularDataResult.values : [];
        executedReport.isLoaded = true;
        executedReport.isEmpty = (response.status === 204);
        d.resolve(executedReport);
    }, d.reject);

    return d.promise();
}

const CONTRIBUTION_METRIC_FORMAT = '#,##0.00%';

const getFilterExpression = listAttributeFilter => {
    const attributeUri = get(listAttributeFilter, 'listAttributeFilter.attribute');
    const elements = get(listAttributeFilter, 'listAttributeFilter.default.attributeElements', []);
    if (isEmpty(elements)) {
        return null;
    }
    const elementsForQuery = map(elements, e => `[${e}]`);
    const negative = get(listAttributeFilter, 'listAttributeFilter.default.negativeSelection') ? 'NOT ' : '';

    return `[${attributeUri}] ${negative}IN (${elementsForQuery.join(',')})`;
};

const getGeneratedMetricExpression = item => {
    const aggregation = get(item, 'aggregation', '').toUpperCase();
    const objectUri = get(item, 'objectUri');
    const where = filter(map(get(item, 'measureFilters'), getFilterExpression), e => !!e);

    return 'SELECT ' + (aggregation ? `${aggregation}([${objectUri}])` : `[${objectUri}]`) +
        (notEmpty(where) ? ` WHERE ${where.join(' AND ')}` : '');
};

const getPercentMetricExpression = (attribute, metricId) => {
    const attributeUri = get(attribute, 'attribute');

    return `SELECT (SELECT ${metricId}) / (SELECT ${metricId} BY ALL [${attributeUri}])`;
};

const getPoPExpression = (attribute, metricId) => {
    const attributeUri = get(attribute, 'attribute');

    return `SELECT (SELECT ${metricId}) FOR PREVIOUS ([${attributeUri}])`;
};

const getGeneratedMetricHash = (title, format, expression) => md5(`${expression}#${title}#${format}`);

const allFiltersEmpty = item => every(map(
    get(item, 'measureFilters', []),
    f => isEmpty(get(f, 'listAttributeFilter.default.attributeElements', []))
));

const getGeneratedMetricIdentifier = (item, aggregation, expressionCreator, hasher) => {
    const [, , , prjId, , id] = get(item, 'objectUri').split('/');
    const identifier = `${prjId}_${id}`;
    const hash = hasher(expressionCreator(item));
    const hasNoFilters = isEmpty(get(item, 'measureFilters', []));
    const type = get(item, 'type');

    const prefix = (hasNoFilters || allFiltersEmpty(item)) ? '' : 'filtered_';

    return `${type}_${identifier}.generated.${prefix}${aggregation}.${hash}`;
};

const isDerived = (item) => {
    const type = get(item, 'type');
    return (type === 'fact' || type === 'attribute' || !allFiltersEmpty(item));
};

const getDate = date => get(date, 'dateFilter', date);

const getCategories = mdObj => map(mdObj.categories, ({ category }) => category);
const getFilters = ({ filters }) => filters;

// @TODO This is fucking stupid name
const getAttribute = mdObj =>
    find([...getCategories(mdObj), ...getFilters(mdObj)], c => (c.type === 'date' || c.dateFilter));

const createPureMetric = measure => ({
    element: get(measure, 'objectUri'),
    sort: !measure.showPoP ? get(measure, 'sort') : null
});

const createDerivedMetric = measure => {
    const { title, format, sort } = measure;

    const hasher = partial(getGeneratedMetricHash, title, format);
    const aggregation = get(measure, 'aggregation', 'base').toLowerCase();
    const element = getGeneratedMetricIdentifier(measure, aggregation, getGeneratedMetricExpression, hasher);
    const definition = {
        metricDefinition: {
            identifier: element,
            expression: getGeneratedMetricExpression(measure),
            title,
            format
        }
    };

    return { element, definition, sort };
};

const createContributionMetric = (measure, mdObj) => {
    const category = first(getCategories(mdObj));

    let generated;
    let getMetricExpression = partial(getPercentMetricExpression, category, `[${get(measure, 'objectUri')}]`);
    if (isDerived(measure)) {
        generated = createDerivedMetric(measure);
        getMetricExpression = partial(getPercentMetricExpression, category, `{${get(generated, 'definition.metricDefinition.identifier')}}`);
    }
    const title = `% ${get(measure, 'title')}`.replace(/^(% )+/, '% ');
    const hasher = partial(getGeneratedMetricHash, title, CONTRIBUTION_METRIC_FORMAT);
    const result = [{
        element: getGeneratedMetricIdentifier(measure, 'percent', getMetricExpression, hasher),
        definition: {
            metricDefinition: {
                identifier: getGeneratedMetricIdentifier(measure, 'percent', getMetricExpression, hasher),
                expression: getMetricExpression(measure),
                title,
                format: CONTRIBUTION_METRIC_FORMAT
            }
        },
        sort: get(measure, 'sort')
    }];

    if (generated) {
        result.unshift({ definition: generated.definition });
    }

    return result;
};

const createPoPMetric = (measure, mdObj) => {
    const title = `${get(measure, 'title')} - previous year`;
    const format = get(measure, 'format');
    const hasher = partial(getGeneratedMetricHash, title, format);

    const date = getDate(getAttribute(mdObj));

    let generated;
    let getMetricExpression = partial(getPoPExpression, date, `[${get(measure, 'objectUri')}]`);

    if (isDerived(measure)) {
        generated = createDerivedMetric(measure);
        getMetricExpression = partial(getPoPExpression, date, `{${get(generated, 'definition.metricDefinition.identifier')}}`);
    }

    const identifier = getGeneratedMetricIdentifier(measure, 'pop', getMetricExpression, hasher);

    const result = [{
        element: identifier,
        definition: {
            metricDefinition: {
                identifier,
                expression: getMetricExpression(),
                title,
                format
            }
        },
        sort: get(measure, 'sort')
    }];

    if (generated) {
        result.push(generated);
    } else {
        result.push(createPureMetric(measure));
    }

    return result;
};

const createContributionPoPMetric = (measure, mdObj) => {
    const category = first(getCategories(mdObj));
    const date = getAttribute(mdObj);

    // TODO: Somehow these are the same thing
    console.log(category);
    console.log(date);

    const generated = createContributionMetric(category ? getDate(category) : date, measure);

    const title = `% ${get(measure, 'title')} - previous year`.replace(/^(% )+/, '% ');
    const format = CONTRIBUTION_METRIC_FORMAT;
    const hasher = partial(getGeneratedMetricHash, title, format);

    const getMetricExpression = partial(getPoPExpression, getDate(date), `{${last(generated).element}}`);

    const identifier = getGeneratedMetricIdentifier(measure, 'pop', getMetricExpression, hasher);

    const result = [{
        element: identifier,
        definition: {
            metricDefinition: {
                identifier,
                expression: getMetricExpression(),
                title,
                format
            }
        },
        sort: get(measure, 'sort')
    }];

    result.push(generated);

    return flatten(result);
};

const categoryToElement = c => ({ element: get(c, 'displayForm'), sort: get(c, 'sort') });

const attributeFilterToWhere = f => {
    const elements = get(f, 'listAttributeFilter.default.attributeElements', []);
    const elementsForQuery = map(elements, e => ({ id: last(e.split('=')) }));

    const dfUri = get(f, 'listAttributeFilter.displayForm');
    const negative = get(f, 'listAttributeFilter.default.negativeSelection');

    return negative ?
        { [dfUri]: { '$not': { '$in': elementsForQuery } } } :
        { [dfUri]: { '$in': elementsForQuery } };
};

const dateFilterToWhere = f => {
    const dimensionUri = get(f, 'dateFilter.dimension');
    const granularity = get(f, 'dateFilter.granularity');
    const between = [get(f, 'dateFilter.from'), get(f, 'dateFilter.to')];
    return { [dimensionUri]: { '$between': between, '$granularity': granularity } };
};

const isPoP = ({ showPoP }) => showPoP;
const isContribution = ({ showInPercent }) => showInPercent;

const isCalculatedMeasure = ({ type }) => type === 'metric';

const rules = new Rules();

rules.addRule(
    [isPoP, isContribution],
    createContributionPoPMetric
);

rules.addRule(
    [isPoP],
    createPoPMetric
);

rules.addRule(
    [isContribution],
    createContributionMetric
);

rules.addRule(
    [isDerived],
    createDerivedMetric
);

rules.addRule(
    [isCalculatedMeasure],
    createPureMetric
);

function getMetricFactory(measure) {
    const factory = rules.match(measure);

    invariant(factory, `Unknown factory for: ${measure}`);

    return factory;
}

const isDateFilterExecutable = dateFilter =>
    get(dateFilter, 'from') !== undefined &&
    get(dateFilter, 'to') !== undefined;

const isAttributeFilterExecutable = listAttributeFilter =>
    notEmpty(get(listAttributeFilter, ['default', 'attributeElements']));


function getWhere({ filters }) {
    const attributeFilters = map(filter(filters, ({ listAttributeFilter }) => isAttributeFilterExecutable(listAttributeFilter)), attributeFilterToWhere);
    const dateFilters = map(filter(filters, ({ dateFilter }) => isDateFilterExecutable(dateFilter)), dateFilterToWhere);

    return [...attributeFilters, ...dateFilters].reduce(assign, {});
}

const sortToOrderBy = item => ({ column: get(item, 'element'), direction: get(item, 'sort') });

export const mdToExecutionConfiguration = (mdObj) => {
    const measures = map(mdObj.measures, ({ measure }) => measure);
    const metrics = flatten(map(measures, measure => getMetricFactory(measure)(measure, mdObj)));

    const categories = map(getCategories(mdObj), categoryToElement);
    const allItems = [...categories, ...metrics];

    return { execution: {
        columns: compact(map(allItems, 'element')),
        orderBy: map(filter(allItems, item => item.sort), sortToOrderBy),
        definitions: compact(map(metrics, 'definition')),
        where: getWhere(mdObj)
    } };
};

export const getDataForVis = (projectId, mdObj) => {
    const { execution } = mdToExecutionConfiguration(get(mdObj, 'buckets'));
    const { columns, ...executionConfiguration } = execution;
    return getData(projectId, columns, executionConfiguration);
};
