(defproject org.toomuchcode/clara-storm "0.1.0-SNAPSHOT"
  :description "Clara Rules -- Storm Support"
  :url "http://rbrush.github.io/clara-storm/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.toomuchcode/clara-rules "0.3.0-SNAPSHOT"]
                 [storm "0.8.2"]]
  :plugins [[codox "0.6.4"]
            [lein-javadoc "0.1.1"]]
  :javadoc-opts {:package-names ["clara.storm"]}
  :source-paths ["src/main/clojure"]
  :test-paths ["src/test/clojure"]
  :java-source-paths ["src/main/java"]
  :scm {:name "git"
        :url "https://github.com/rbrush/clara-storm.git"}
  :pom-addition [:developers [:developer {:id "rbrush"}
                              [:name "Ryan Brush"]
                              [:url "http://www.toomuchcode.org"]]]
  :repositories [["snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots/"}]]
  :deploy-repositories [["snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots/"
                                      :creds :gpg}]])
