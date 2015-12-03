/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.groovy;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Groovy interpreter for Zeppelin.
 *
 */

public class GroovySparkInterpreter extends Interpreter {
  Logger logger = LoggerFactory.getLogger(GroovySparkInterpreter.class);

  private GroovyShell interpreter;
  private Binding binding;

  static {
    Interpreter.register("groovy", GroovySparkInterpreter.class.getName());
  }

  public GroovySparkInterpreter(Properties property) {
    super(property);

    binding = new Binding();
//    binding.setVariable("sc", sc);
//    binding.setVariable("sqlc", sqlc);
//    binding.setVariable("z", z);
//    binding.setVariable("out", printStream);

    CompilerConfiguration compilerConfiguration = new CompilerConfiguration();
    //compilerConfiguration.setClasspath();
    //compilerConfiguration.setOutput(new PrintWriter());

    interpreter = new GroovyShell(binding, compilerConfiguration);
    logger.info("GroovySparkInterpreter<open>: interpreter", interpreter);
  }

  @Override
  public void open() {
    interpreter = new GroovyShell();
  }

  @Override
  public void close() {}

  @Override
  public InterpreterResult interpret(String script, InterpreterContext interpreterContext) {
    String html;
    logger.info("GroovySparkInterpreter<interpret>: ", script);

    System.out.println("GroovySparkInterpreter<interpret>: " + script);
    if (script == null || script.trim().length() == 0) {
      return new InterpreterResult(Code.SUCCESS);
    }

    PrintStream originalOut = System.out;
    PrintStream originalErr = System.err;

    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    PrintStream printStream = null;
    try {
      printStream = new PrintStream(stream, true, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }

    StringWriter stacktrace = new StringWriter();
    PrintWriter errWriter = new PrintWriter(stacktrace);

    System.setOut(printStream);
    System.setErr(printStream);

    Code r = null;

    Object result = null;
    try {
      result = interpreter.evaluate(script);
    } catch (MultipleCompilationErrorsException e) {
      stacktrace.append(e.getMessage());
    } catch (Throwable t) {
//      sanitizeStacktrace(t);
//      Throwable cause = t;
//      while ((cause = cause.getCause()) != null) {
//        sanitizeStacktrace(cause);
//      }
      t.printStackTrace(errWriter);
    } finally {
      System.setOut(originalOut);
      System.setErr(originalErr);
    }

    System.out.println("" + stacktrace);

    if (stacktrace.toString().length() > 0) {
      return new InterpreterResult(Code.ERROR, "" + new String(stream.toByteArray()) + "\n" +
        stacktrace.toString());
    } else {
      return new InterpreterResult(Code.SUCCESS, "" + new String(stream.toByteArray()));
    }
  }

  @Override
  public void cancel(InterpreterContext context) {}

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetParallelScheduler(
      GroovySparkInterpreter.class.getName() + this.hashCode(), 5);
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    return null;
  }

  private Throwable sanitizeStacktrace(Throwable t) {
    String filtered[] = {"com.google.", "org.mortbay.", "java.", "javax.", "sun.", "groovy.",
      "org.codehaus.groovy.", "executor"
    };

    StackTraceElement[] trace = t.getStackTrace();
    List<StackTraceElement> newTrace = new ArrayList<StackTraceElement>();
    for (StackTraceElement ste: trace) {
      boolean include = true;
      for (String prefix: filtered){
        if (ste.getClassName().startsWith(prefix)) include = false;
      }
      if (include) newTrace.add(ste);
    }
    StackTraceElement[] clean = newTrace.toArray(new StackTraceElement[newTrace.size()]);

    t.setStackTrace(clean);

    return t;
  }
}
