/**
 * Physical job planning for the local Link-Up runtime.
 *
 * <p>Planner components transform prepared connector/job inputs into immutable
 * execution plans. They may calculate topology, parallelism, and split
 * assignment, but must not start threads or execute source/sink I/O.
 */
package com.link.up.framework.planner;
