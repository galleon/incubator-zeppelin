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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.apache.zeppelin.groovy.GroovySparkInterpreter;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Shell interpreter for Zeppelin.
 *
 * @author galleon
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GroovySparkInterpreterTest {
  public static GroovySparkInterpreter repl;
  private InterpreterContext context;
  private File tmpDir;


  /**
   * Get spark version number as a numerical value.
   * eg. 1.1.x => 11, 1.2.x => 12, 1.3.x => 13 ...
   */
  public static int getSparkVersionNumber() {
    if (repl == null) {
      return 0;
    }

//    String[] split = repl.getSparkContext().version().split("\\.");
//    int version = Integer.parseInt(split[0]) * 10 + Integer.parseInt(split[1]);
//    return version;
    return 15;
  }

  @Before
  public void setUp() throws Exception {
    tmpDir = new File(System.getProperty("java.io.tmpdir") + "/ZeppelinLTest_" + System.currentTimeMillis());
    System.setProperty("zeppelin.dep.localrepo", tmpDir.getAbsolutePath() + "/local-repo");

    tmpDir.mkdirs();

    if (repl == null) {
      Properties p = new Properties();

      repl = new GroovySparkInterpreter(p);
      repl.open();
    }

    InterpreterGroup intpGroup = new InterpreterGroup();
    context = new InterpreterContext("note", "id", "title", "text",
      new HashMap<String, Object>(), new GUI(), new AngularObjectRegistry(
      intpGroup.getId(), null),
      new LinkedList<InterpreterContextRunner>());
  }

  @After
  public void tearDown() throws Exception {
    delete(tmpDir);
  }

  private void delete(File file) {
    if (file.isFile()) file.delete();
    else if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files != null && files.length > 0) {
        for (File f : files) {
          delete(f);
        }
      }
      file.delete();
    }
  }

  @Test
  public void testBasicIntp() {
    assertEquals(InterpreterResult.Code.SUCCESS,
      repl.interpret("def a = 1\ndef b = 2", context).code());

    // when interpret incomplete expression
    //InterpreterResult incomplete = repl.interpret("val a = \"\"\"", context);
    //assertEquals(InterpreterResult.Code.INCOMPLETE, incomplete.code());
    //assertTrue(incomplete.message().length() > 0); // expecting some error
    // message
    /*
     * assertEquals(1, repl.getValue("a")); assertEquals(2, repl.getValue("b"));
     * repl.interpret("val ver = sc.version");
     * assertNotNull(repl.getValue("ver")); assertEquals("HELLO\n",
     * repl.interpret("println(\"HELLO\")").message());
     */
  }
}
